const {test} = require('node:test');
const assert = require('node:assert/strict');
const V = require('../web/static/js/long_term_viewport.js');
test('full range and presets contain the intended real monthly points', () => {
    assert.deepEqual(V.preset('all',105,36), {start:0,end:140});
    assert.deepEqual(V.preset('five-years',105,36), {start:45,end:104});
    assert.deepEqual(V.preset('recent-forecast',105,36), {start:81,end:140});
    assert.deepEqual(V.preset('five-years',105,36,true), {start:0,end:35});
});
test('zoom preserves the month under the pointer until a boundary is reached', () => {
    const range={start:20,end:100}, anchor=.25;
    const result=V.zoom(range,141,.5,anchor);
    assert.equal(result.start + (result.end-result.start)*anchor,40);
    assert.deepEqual(result,{start:30,end:70});
});
test('zoom is bounded to six monthly points and never leaves the dataset', () => {
    assert.deepEqual(V.zoom({start:0,end:140},141,.0001,0),{start:0,end:5});
    assert.deepEqual(V.zoom({start:120,end:140},141,100,1),{start:0,end:140});
    assert.deepEqual(V.zoom({start:0,end:2},3,.001),{start:0,end:2});
});
test('pan clamps at both ends while preserving the visible duration', () => {
    assert.deepEqual(V.pan({start:20,end:40},141,-100),{start:0,end:20});
    assert.deepEqual(V.pan({start:20,end:40},141,200),{start:120,end:140});
});
test('historical/future boundary has correct offscreen states', () => {
    assert.equal(V.boundary({start:0,end:104},105),'future-right');
    assert.equal(V.boundary({start:105,end:140},105),'past-left');
    assert.equal(V.boundary({start:81,end:140},105),'visible');
});
test('navigation returns new ranges without mutating saved per-mode state', () => {
    const forecast={start:81,end:140}, backtest={start:0,end:35};
    V.pan(forecast,141,-10); V.zoom(backtest,36,.5);
    assert.deepEqual(forecast,{start:81,end:140}); assert.deepEqual(backtest,{start:0,end:35});
});
