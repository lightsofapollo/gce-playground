/**
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var proxy = require('proxied-promise-object');
var gcloud = require('gcloud')({
  projectId: process.env.GCLOUD_PROJECT_ID
});

var slug = require('slugid');

const KIND = 'task';

async function main() {

  var datastore = gcloud.datastore;
  var dataset = datastore.dataset({
    projectId: process.env.GCLOUD_PROJECT_ID,
    version: 'v1beta2',
    namespace: 'testing'
  });

  var service = proxy(dataset);
  async function transaction(asyncFn) {
    let finished, trans;
    await new Promise((accept, reject) => {
      dataset.runInTransaction((trans, done) => {
        let proxyTrans = proxy(trans);
        asyncFn(proxyTrans).
          then(() => done()).
          catch((err) => done(err));
      }, function(err) {
        if (err) return reject(err);
        accept();
      });
    });
  }

  // create a entity group of 1k objects
  var iters = 10;
  var number = 500;
  var ops = [];

  while(iters--) {
    var root = slug.v4();
    var objects = [];

    for (var i = 0; i < number; i++) {
      var trans = proxy((await service.runInTransaction))
      let key = dataset.key([KIND, root, KIND, String(i+1)])

      objects.push({
        key: key,
        data: {
          state: 'pending',
          payload: {
            command: ['ls']
          }
        }
      });
    }

    // Creates the objects and assigns the ids to them...
    console.time('create');
    let inserted = await service.save(objects[0]);
    console.timeEnd('create');

    // run each update in it's own transaction...

    // iterate over all the objects and mark them as running state (in parallel)
    console.time('update');
    await transaction(async (transaction) => {
      let updates = [];
      console.time('get');
      await Promise.all(objects.map(async (original) => {
        let obj = await transaction.get(original.key);
        obj.data.state = 'running';
        updates.push(obj);
      }));
      console.timeEnd('get');
      await transaction.update(updates);
    });
    console.timeEnd('update');
  }
}

main().catch((err) => {
  setTimeout(() => {
    throw err;
  });
});
