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

const KIND = 'taskfoo2';

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
          then(() => {
            done();
          }).
          catch((err) => {
            // This is pretty ugly but implements auto-rollback on transaction
            // errors and proper propagation of errors via the promise chain.
            return proxyTrans.rollback().
              then(() => {
                done(err);
                reject(err);
              }).
              catch((rollbackErr) => {
                done(rollbackErr);
                reject(rollbackErr);
              });
          });
      }, function(err) {
        if (err) return reject(err);
        accept();
      });
    });
  }

  // create a entity group of 1k objects
  var iters = 50;
  var number = 500;
  var ops = [];

  var insertOpts = [];

  console.time(`insert`);
  while(iters--) {
    let root = slug.v4();
    let trans = proxy((await service.runInTransaction))
    let objects = [];
    insertOpts.push(transaction(async (trans) => {
      for (let i = 0; i < number; i++) {
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
      trans.save(objects);
    }));
  }
  console.log('inserted', iters * number);
  await Promise.all(insertOpts);
  console.timeEnd(`insert`);
}

main().catch((err) => {
  setTimeout(() => {
    throw err;
  });
});
