import test from 'node:test';
import assert from 'node:assert/strict';
import { buildOfferFields, validateOffer } from '../src/offers.mjs';
import { postToGmb, commitScheduledPosts, enqueueScheduledPost } from '../src/gmb.mjs';
import worker from '../src/worker.mjs';

const valid = { postText: 'Actual offer description.', topicType: 'OFFER', offerTitle: '30% off painting labour with whole-house removal', offerStart: '2099-11-01', offerEnd: '2099-11-30', offerCoupon: 'TOGETHER30', offerRedeemUrl: 'https://example.com/offer', offerTerms: 'Painting labour only. Materials excluded.' };
function env() {
 const values = new Map(Object.entries({ profiles: [{ profileId: 'p', accountId: 'a', locationId: 'l', businessName: 'Ceiling removal', defaults: {} }], tokens: { access_token: 'test', expiry_date: Date.now()+3600000 } }).map(([k,v])=>[k,JSON.stringify(v)]));
 const writes=[];
 return { values, writes, D1_DB: { prepare(sql) { let args=[]; return { bind(...a) {args=a;return this;}, async first(){return values.has(args[0])?{value:values.get(args[0])}:null;}, async all(){return {results:[]};}, async run(){writes.push({sql,args});if(sql.includes('INSERT INTO kv'))values.set(args[0],args[1]);return {success:true};} }; } } };
}
test('offer uses documented Google event and offer fields', () => {
 assert.deepEqual(buildOfferFields(valid), { event:{title:valid.offerTitle,schedule:{startDate:{year:2099,month:11,day:1},startTime:{hours:0,minutes:0,seconds:0},endDate:{year:2099,month:11,day:30},endTime:{hours:23,minutes:59,seconds:59}}}, offer:{couponCode:'TOGETHER30',redeemOnlineUrl:valid.offerRedeemUrl,termsConditions:valid.offerTerms} });
 assert.deepEqual(buildOfferFields({...valid,offerCoupon:'',offerRedeemUrl:'',offerTerms:''}).offer,{});
});
test('missing dates, impossible dates, reversed dates and unsafe links rejected', () => {
 for (const patch of [{offerTitle:''},{offerTitle:'x'.repeat(59)},{offerStart:''},{offerEnd:'2099-02-29'},{offerEnd:'2099-10-01'},{offerRedeemUrl:'javascript:alert(1)'},{offerRedeemUrl:'https://user:pass@example.com'}]) assert.throws(()=>buildOfferFields({...valid,...patch}));
 assert.doesNotThrow(()=>buildOfferFields({...valid,offerStart:'2096-02-29',offerEnd:'2096-02-29'}));
});
test('expiry and scheduling use Toronto dates at the UTC date boundary', () => {
 const b={...valid,offerStart:'2026-11-01',offerEnd:'2026-11-30'};
 assert.doesNotThrow(()=>validateOffer(b,'2026-12-01T04:59:00Z',new Date('2026-11-01T12:00:00Z')));
 assert.throws(()=>validateOffer(b,'2026-12-01T05:00:00Z',new Date('2026-11-01T12:00:00Z')),/end date/);
 assert.throws(()=>validateOffer(b,null,new Date('2026-12-01T05:00:00Z')),/expired/);
 assert.doesNotThrow(()=>validateOffer({topicType:'STANDARD'}));
});
test('invalid offer returns HTTP 400 before any Google request', async t=>{
 t.mock.method(globalThis,'fetch',()=>{throw new Error('Must not call Google');});
 const r=await worker.fetch(new Request('https://app.test/post-now',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({profileId:'p',topicType:'OFFER'})}),env(),{});
 assert.equal(r.status,400);assert.match((await r.json()).error,/title/);
});
test('batch validation rejects all items before any partial queue write', async()=>{
 const e=env();
 await assert.rejects(commitScheduledPosts(e,[{profileId:'p',runAt:'2099-11-02T15:00:00Z',body:valid},{profileId:'p',runAt:'2099-11-03T15:00:00Z',body:{...valid,offerEnd:''}}]));
 assert.equal(e.writes.length,0);
 await assert.rejects(enqueueScheduledPost(e,{profileId:'p',runAt:'2099-12-01T15:00:00Z',body:valid}));
 assert.equal(e.writes.length,0);
});
test('queue retains all offer details',async()=>{
 const e=env();const item=await enqueueScheduledPost(e,{profileId:'p',runAt:'2099-11-01T15:00:00Z',body:valid});
 assert.deepEqual(item.body,valid);
 assert.equal(JSON.parse(e.writes.find(x=>x.sql.includes('INSERT INTO scheduled_posts')).args[4]).offerTerms,valid.offerTerms);
});
for(const type of ['OFFER','STANDARD'])test(`${type} publishing sends the correct Google payload`,async t=>{
 const e=env();let sent;
 t.mock.method(globalThis,'fetch',async(url,init)=>{
  if(url.includes('mybusinessbusinessinformation')) return Response.json({websiteUri:'https://example.com'});
  assert.equal(url,'https://mybusiness.googleapis.com/v4/accounts/a/locations/l/localPosts');
  sent=JSON.parse(init.body);return Response.json({name:'accounts/a/locations/l/localPosts/1',state:'LIVE',searchUrl:'https://google.com/post'});
 });
 await postToGmb(e,{...valid,topicType:type,profileId:'p',postText:'A specific promotion.',cta:type==='OFFER'?'CALL_NOW':'LEARN_MORE',linkUrl:'https://example.com',mediaUrl:'https://example.com/photo.png'});
 assert.equal(sent.topicType,type);assert.equal(sent.summary,'A specific promotion.');assert.equal(sent.media[0].sourceUrl,'https://example.com/photo.png');
 if(type==='OFFER'){assert.equal(sent.callToAction,undefined);assert.deepEqual(sent.offer,buildOfferFields(valid).offer);assert.deepEqual(sent.event,buildOfferFields(valid).event);}
 else{assert.equal(sent.offer,undefined);assert.equal(sent.callToAction.url,'https://example.com');}
});
