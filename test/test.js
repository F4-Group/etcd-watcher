var assert = require('assert');
var _ = require('underscore');
var async = require('async');
var Etcd = require('node-etcd');

var etcd = new Etcd();
var etcdWatcher = require('../lib');

async.series([
    testAllRequired,
    testAllOptional,
    testLegacy,
], function (err) {
    assert.ifError(err);
    console.log('OK');
    process.exit(0);
});

function testAllRequired(cb) {
    var options = {
        '/test-ew/1': {
            required: true,
        },
        '/test-ew/2': {
            required: true,
        }
    };
    var keys = _.keys(options);
    var watcher = etcdWatcher.watcher(etcd, options);
    async.series([
        _.bind(etcd.del, etcd, keys[0]),
        _.bind(etcd.del, etcd, keys[1]),
    ], function (err) {
        var expect = expectSeries(watcher, [
            { '/test-ew/1': 'aa', '/test-ew/2': 'bb' },
            { '/test-ew/1': 'aa', '/test-ew/2': 'bc' },
        ]);
        async.series([
            _.bind(etcd.set, etcd, keys[0], 'aa'),
            _.bind(etcd.set, etcd, keys[1], 'bb'),
            _.bind(etcd.set, etcd, keys[1], 'bc'),
        ], function (err) {
            assert.ifError(err);
            expect.checkAllReceived(cb);
        });
    });
}

function testAllOptional(cb) {
    var options = {
        '/test-ew/3': {
            default: 'def3',
        },
        '/test-ew/4': {
            default: 'def4',
        }
    };
    var keys = _.keys(options);
    var watcher = etcdWatcher.watcher(etcd, options);
    async.series([
        _.bind(etcd.del, etcd, keys[0]),
        _.bind(etcd.del, etcd, keys[1]),
    ], function (err) {
        var expect = expectSeries(watcher, [
            { '/test-ew/3': 'def3', '/test-ew/4': 'def4' },
            { '/test-ew/3': 'aa', '/test-ew/4': 'def4' },
            { '/test-ew/3': 'aa', '/test-ew/4': 'bb' },
            { '/test-ew/3': 'aa', '/test-ew/4': 'bc' },
        ]);
        async.series([
            _.bind(etcd.set, etcd, keys[0], 'aa'),
            _.bind(etcd.set, etcd, keys[1], 'bb'),
            _.bind(etcd.set, etcd, keys[1], 'bc'),
        ], function (err) {
            assert.ifError(err);
            expect.checkAllReceived(cb);
        });
    });
}

function testLegacy(cb) {
    var keys = ['/test-ew/legacy/0', '/test-ew/legacy/1'];
    var watcher = etcdWatcher.watcher(etcd, keys);

    async.series([
        _.bind(etcd.del, etcd, keys[0]),
        _.bind(etcd.del, etcd, keys[1]),
    ], function (err) {
        var expect = expectSeries(watcher, [
            { '/test-ew/legacy/0': 'aa', '/test-ew/legacy/1': 'bb' },
            { '/test-ew/legacy/0': 'aa', '/test-ew/legacy/1': 'bc' },
        ]);
        async.series([
            _.bind(etcd.set, etcd, keys[0], 'aa'),
            _.bind(etcd.set, etcd, keys[1], 'bb'),
            _.bind(etcd.set, etcd, keys[1], 'bc'),
        ], function (err) {
            assert.ifError(err);
            expect.checkAllReceived(cb);
        });
    });
}

function expectSeries(watcher, values) {
    var expectedIndex = 0;
    watcher.wait(function (err, values) {
        assert.ifError(err);
        next(values);
    });
    watcher.on('change', function (values) {
        assert.notEqual(expectedIndex, 0);
        next(values);
    });

    return {
        checkAllReceived: checkAllReceived,
    };

    function next(value) {
        var expected = values[expectedIndex];
        if (_.isObject(expected))
            assert.deepEqual(value, expected);
        else
            assert.equal(value, expected);
        ++expectedIndex;
    }

    function checkAllReceived(cb) {
        setTimeout(function () {
            assert.equal(expectedIndex, values.length, 'not all expected values received');
            cb();
        }, 1000);
    }
}
