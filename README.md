# etcd-watcher, watch etcd key creation and subsequent changes

## Install

You need [node-etcd](http://github.com/stianeikeland/node-etcd).

```
$ npm install node-etcd
$ npm install etcd-watcher
```

## Usage

```js
var Etcd = require('node-etcd');
var etcd = new Etcd();
var etcdWatcher = require('etcd-watcher');

var watcher = etcdWatcher.watcher(etcd, {
    key1: {
        required: true,
        etcd: '/foo/bar/key1' 
    },
    key2: {
        required: false,
        default: 'defaultValue'
    }
});
watcher.wait(function (err, values) {
    console.log('keys set', values);
    watcher.on('change', function (values) {
        console.log('key change', values);
    })
});
```
