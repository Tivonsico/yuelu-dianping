import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {config,connections,seedTokens,seedVoucher,load,drain} from './common.mjs';

const mode=process.env.MODE||process.argv[2]||'stream';
if(!['direct','cache','sync','stream'].includes(mode))throw Error('MODE: direct|cache|sync|stream');
const count=Number(process.env.COUNT||3000), rounds=Number(process.env.ROUNDS||3);
const levels=(process.env.CONCURRENCY||'1,32,128,256').split(',').map(Number);
const root=path.resolve(process.env.RESULT_DIR||'results');fs.mkdirSync(root,{recursive:true});
const conn=await connections();const {db,redis}=conn;
const runId=process.env.RUN_ID||new Date().toISOString().replace(/[:.]/g,'-');
const folder=path.join(root,runId+'-'+mode);fs.mkdirSync(folder,{recursive:true});
fs.writeFileSync(path.join(folder,'environment.json'),JSON.stringify({recordedAt:new Date().toISOString(),mode,config:{...config,mysql:{...config.mysql,password:'[test credential]'},redis:{url:'[isolated redis]'}},count,rounds,levels,node:process.version,os:os.type()+' '+os.release(),cpu:os.cpus()[0]?.model,logicalCpus:os.cpus().length,totalMemory:os.totalmem(),semantics:{direct:'Reconstructed direct MySQL shop baseline, authentication same as cache',cache:'Actual /shop/1 after successful warmup',sync:'Reconstructed synchronous baseline: Redisson + existing transactional getResult; no Redis stock reservation',stream:'Current Redis Stream-only implementation; HTTP acceptance is not SQL completion',load:'Closed-loop fixed total requests; client, application and dependencies share one machine; latency includes transport; first warmup excluded'}},null,2));
const results=[];
try {
  await seedTokens(redis,Math.max(count,500));
  const cache=mode==='direct'||mode==='cache';
  const endpoint=id=>cache?(mode==='direct'?'/bench/shop/1':'/shop/1'):(mode==='sync'?'/bench/sync/'+id:'/voucher-order/seckill/'+id);
  // Explicit successful warmup, a separate voucher prevents measurement contamination.
  if(!cache)await seedVoucher(db,redis,999990,500);
  const warm=await load({name:'warmup',path:endpoint(999990),method:cache?'GET':'POST',count:500,concurrency:32,users:500,record:false});
  if(warm.accepted!==500||warm.httpErrors)throw Error('Warmup failed: '+JSON.stringify(warm));
  if(!cache){const settled=await drain(db,redis,999990,500);if(settled.timedOut)throw Error('Warmup drain timeout');}
  for(let round=1;round<=rounds;round++)for(const concurrency of levels) {
    const voucherId=900000+round*1000+concurrency;
    if(!cache)await seedVoucher(db,redis,voucherId,count);
    const label=`${mode}-r${round}-c${concurrency}`;
    const result=await load({name:label,path:endpoint(voucherId),method:cache?'GET':'POST',count,concurrency,users:count,output:path.join(folder,label+'.jsonl')});
    result.mode=mode;result.round=round;
    if(!cache){const settled=await drain(db,redis,voucherId,result.accepted);Object.assign(result,settled);result.durableQps=settled.audit.orders/((result.durationMs+settled.drainMs)/1000);result.consistent=!settled.timedOut&&settled.audit.orders===result.accepted&&settled.audit.duplicateUsers===0&&settled.audit.mysqlStock===count-result.accepted&&(mode==='sync'||settled.audit.redisStock===count-result.accepted);}
    else result.consistent=result.accepted===count&&result.httpErrors===0;
    results.push(result);fs.writeFileSync(path.join(folder,'summary.json'),JSON.stringify(results,null,2));
    const cols=['name','mode','round','count','concurrency','durationMs','requestQps','accepted','acceptedQps','businessRejects','httpErrors','meanMs','p50Ms','p95Ms','p99Ms','drainMs','durableQps','consistent'];
    fs.writeFileSync(path.join(folder,'summary.csv'),cols.join(',')+'\n'+results.map(r=>cols.map(c=>r[c]??'').join(',')).join('\n')+'\n');
    console.log(JSON.stringify(result));
    if(!result.consistent||result.httpErrors)throw Error('Measurement consistency failed; retained all raw data');
  }
} finally {await conn.close();}
