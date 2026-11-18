import fs from 'node:fs';
import {connections,seedTokens,seedVoucher,load,drain,audit,client,sleep} from './common.mjs';
fs.mkdirSync('results',{recursive:true});
const c=await connections(),http=client(128),checks=[];
async function save(name,pass,evidence){checks.push({name,pass,evidence});fs.writeFileSync('results/stream-smoke.json',JSON.stringify({at:new Date().toISOString(),checks},null,2));if(!pass)throw Error(name);}
async function waitEmpty(){for(let i=0;i<200;i++){const groups=await c.redis.sendCommand(['XINFO','GROUPS','stream.orders']);const g=groups[0];if(Number(g[g.indexOf('pending')+1])===0&&Number(g[g.indexOf('lag')+1])===0)return;await sleep(50);}throw Error('Stream did not drain');}
try {
  await seedTokens(c.redis,1000);
  await c.redis.del('cache:shop:1');const shop=await http.request('/shop/1');
  const absent=await http.request('/shop/999999');
  await save('cold-and-negative-cache',shop.success&&!absent.success&&(await c.redis.get('cache:shop:999999'))==='',{shop,absent});
  await seedVoucher(c.db,c.redis,995001,100);
  const finite=await load({name:'finite',path:'/voucher-order/seckill/995001',method:'POST',count:600,concurrency:128,output:'results/finite.jsonl'});
  const fd=await drain(c.db,c.redis,995001,100);await waitEmpty();
  await save('finite-stock',finite.accepted===100&&finite.httpErrors===0&&fd.audit.orders===100&&fd.audit.mysqlStock===0&&fd.audit.redisStock===0&&fd.audit.duplicateUsers===0,{load:finite,...fd});
  await seedVoucher(c.db,c.redis,995002,200);
  const duplicate=await load({name:'same-user',path:'/voucher-order/seckill/995002',method:'POST',count:500,users:1,concurrency:128,output:'results/same-user.jsonl'});
  const dd=await drain(c.db,c.redis,995002,1);await waitEmpty();
  await save('one-person-one-order',duplicate.accepted===1&&duplicate.httpErrors===0&&dd.audit.orders===1&&dd.audit.mysqlStock===199&&dd.audit.redisStock===199,{load:duplicate,...dd});
  const [[order]]=await c.db.query('SELECT CAST(id AS CHAR) id FROM tb_voucher_order WHERE voucher_id=995002 LIMIT 1');
  for(let i=0;i<40;i++)await c.redis.xAdd('stream.orders','*',{id:i<20?order.id:String(800000000000000000n+BigInt(i)),userId:'1000000',voucherId:'995002'});
  await waitEmpty();const repeated=await audit(c.db,c.redis,995002);
  await save('duplicate-delivery',repeated.orders===1&&repeated.mysqlStock===199&&repeated.redisStock===199&&repeated.duplicateUsers===0,{published:40,audit:repeated});
  await seedVoucher(c.db,c.redis,995003,1);await c.db.query('UPDATE tb_seckill_voucher SET stock=0 WHERE voucher_id=995003');
  const accepted=await http.request('/voucher-order/seckill/995003','bench-2','POST');await sleep(500);
  const during=await audit(c.db,c.redis,995003);
  await save('failed-consumer-keeps-pending',accepted.success&&during.orders===0&&during.mysqlStock===0&&Number(during.streamPending[0])>0,{request:accepted,audit:during});
  await c.db.query('UPDATE tb_seckill_voucher SET stock=1 WHERE voucher_id=995003');
  const recovered=await drain(c.db,c.redis,995003,1);await waitEmpty();
  await save('pending-retry-after-repair',!recovered.timedOut&&recovered.audit.orders===1&&recovered.audit.mysqlStock===0&&recovered.audit.redisStock===0,recovered);
  console.log(JSON.stringify({pass:true,checks:checks.length}));
}finally{http.close();await c.close();}
