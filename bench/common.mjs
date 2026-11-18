import mysql from 'mysql2/promise';
import { createClient } from 'redis';
import http from 'node:http';
import fs from 'node:fs';
import { performance } from 'node:perf_hooks';

export const config = {
  mysql: {host:'127.0.0.1',port:23316,user:'hmdp',password:'hmdp-test',database:'hmdp_bench',supportBigNumbers:true,bigNumberStrings:true},
  redis: {url:'redis://:hmdp-test@127.0.0.1:26389/0'},
  app: process.env.APP_URL || 'http://127.0.0.1:18081',
};
export const sleep = ms => new Promise(resolve => setTimeout(resolve,ms));
export async function connections() {
  const db = await mysql.createConnection(config.mysql);
  const redis = createClient(config.redis);
  redis.on('error',e=>console.error('Redis:',e.message));
  await redis.connect();
  return {db,redis,close:async()=>{await redis.quit();await db.end();}};
}
export async function seedTokens(redis,count=8000) {
  for(let start=0;start<count;start+=500){
    const p=redis.multi();
    for(let i=start;i<Math.min(count,start+500);i++) {
      p.hSet('login:token:bench-'+i,{id:String(1000000+i),nickName:'bench-'+i,icon:''});
      p.expire('login:token:bench-'+i,7200);
    }
    await p.exec();
  }
}
export async function seedVoucher(db,redis,id,stock) {
  if(id<900000 || id>999999) throw Error('Benchmark voucher IDs must be within 900000..999999');
  await db.execute('DELETE FROM tb_voucher_order WHERE voucher_id=?',[id]);
  await db.execute("INSERT INTO tb_voucher (id,shop_id,title,pay_value,actual_value,type,status) VALUES (?,1,'isolated-benchmark',1,1,1,1) ON DUPLICATE KEY UPDATE title=VALUES(title)",[id]);
  await db.execute('INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES (?,?,DATE_SUB(NOW(),INTERVAL 1 DAY),DATE_ADD(NOW(),INTERVAL 1 DAY)) ON DUPLICATE KEY UPDATE stock=VALUES(stock),begin_time=VALUES(begin_time),end_time=VALUES(end_time)',[id,stock]);
  await redis.set('seckill:stock:'+id,String(stock));
  await redis.del('seckill:order:'+id);
}
export function client(maxSockets=256) {
  const agent=new http.Agent({keepAlive:true,maxSockets,maxFreeSockets:maxSockets});
  return {
    request(path,token='bench-0',method='GET') {
      const start=performance.now();
      return new Promise(resolve=>{
        const req=http.request(new URL(path,config.app),{agent,method,headers:{authorization:token}},res=>{
          let body='';res.setEncoding('utf8');res.on('data',chunk=>body+=chunk);
          res.on('end',()=>{
            let parsed;try{parsed=JSON.parse(body);}catch{}
            const orderId=body.match(/"data"\s*:\s*(\d{10,})/)?.[1];
            resolve({latencyMs:performance.now()-start,status:res.statusCode,success:parsed?.success===true,error:parsed?.errorMsg??null,orderId,body:parsed?undefined:body.slice(0,300)});
          });
        });
        req.setTimeout(30000,()=>req.destroy(Error('request timeout')));
        req.on('error',e=>resolve({latencyMs:performance.now()-start,status:0,success:false,error:e.message}));req.end();
      });
    },close(){agent.destroy();}
  };
}
export async function load({name,path,method='GET',count,concurrency,users=count,output,record=true}) {
  const transport=client(concurrency), rows=new Array(count);let cursor=0;
  const startedAt=new Date().toISOString(), start=performance.now();
  await Promise.all(Array.from({length:concurrency},async()=>{
    while(true){const i=cursor++;if(i>=count)return;
      const result=await transport.request(path,'bench-'+(i%users),method);
      rows[i]={request:i,startedOffsetMs:performance.now()-start-result.latencyMs,...result};
    }
  }));
  const durationMs=performance.now()-start;transport.close();
  if(record&&output)fs.writeFileSync(output,rows.map(row=>JSON.stringify(row)).join('\n')+'\n');
  const sorted=rows.map(r=>r.latencyMs).sort((a,b)=>a-b), percentile=p=>sorted[Math.min(sorted.length-1,Math.ceil(p*sorted.length)-1)];
  const rejects={}, errors={};let accepted=0,httpErrors=0;
  for(const r of rows){if(r.success)accepted++;else if(r.status===200)rejects[r.error||'unknown']=(rejects[r.error||'unknown']||0)+1;else {httpErrors++;errors[r.error||String(r.status)]=(errors[r.error||String(r.status)]||0)+1;}}
  return {name,startedAt,count,concurrency,users,durationMs,requestQps:count/(durationMs/1000),accepted,acceptedQps:accepted/(durationMs/1000),businessRejects:count-accepted-httpErrors,rejectReasons:rejects,httpErrors,errorReasons:errors,meanMs:sorted.reduce((a,b)=>a+b,0)/count,p50Ms:percentile(.5),p95Ms:percentile(.95),p99Ms:percentile(.99)};
}
export async function queues() { return []; }
export async function audit(db,redis,voucherId) {
  const [[orders]]=await db.execute('SELECT COUNT(*) orders,COUNT(DISTINCT user_id) distinctUsers FROM tb_voucher_order WHERE voucher_id=?',[voucherId]);
  const [[stock]]=await db.execute('SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?',[voucherId]);
  let pending;try{pending=await redis.sendCommand(['XPENDING','stream.orders','g1']);}catch(e){pending={error:e.message};}
  let groups;try{groups=await redis.sendCommand(['XINFO','GROUPS','stream.orders']);}catch(e){groups={error:e.message};}
  return {voucherId,orders:Number(orders.orders),distinctUsers:Number(orders.distinctUsers),duplicateUsers:Number(orders.orders)-Number(orders.distinctUsers),mysqlStock:stock?.stock,redisStock:Number(await redis.get('seckill:stock:'+voucherId)),reservedUsers:await redis.sCard('seckill:order:'+voucherId),streamPending:pending,streamGroups:groups,queues:await queues()};
}
export async function drain(db,redis,voucherId,accepted,timeoutMs=180000) {
  const start=performance.now();let last,firstCompletionObservedMs;
  while(performance.now()-start<timeoutMs) {
    const [[r]]=await db.execute('SELECT COUNT(*) n FROM tb_voucher_order WHERE voucher_id=?',[voucherId]);
    if(Number(r.n)===accepted){
      if(firstCompletionObservedMs===undefined)firstCompletionObservedMs=performance.now()-start;
      last=await audit(db,redis,voucherId);
      const outstanding=last.queues.filter(q=>!q.name.endsWith('.dlq')).reduce((n,q)=>n+(q.messages||0),0);
      const pending=Array.isArray(last.streamPending)?Number(last.streamPending[0]):0;
      if(outstanding===0&&pending===0)return {drainMs:firstCompletionObservedMs,quiescenceMs:performance.now()-start,timedOut:false,audit:last,observation:'SQL completion checked at 100ms polling intervals; durableQps is observed lower bound'};
    }
    await sleep(100);
  }
  return {drainMs:performance.now()-start,timedOut:true,audit:await audit(db,redis,voucherId)};
}
