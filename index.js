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

async function main() {

  var datastore = gcloud.datastore;
  var dataset = datastore.dataset({
    projectId: process.env.GCLOUD_PROJECT_ID,
    version: 'v1beta2'
  });
  var service = proxy(dataset);

  // create a entity group of 1k objects
  var iters = 5;
  var number = 500;
  var ops = [];

  while(iters--) {
    var root = slug.v4();
    var objects = [];

    for (var i = 0; i < number; i++) {
      console.time('create');
      objects.push({
        key: dataset.key([root, number]),
        data: {
          state: 'pending',
          payload: {
            command: ['ls']
          }
        }
      });
    }

    // Creates the objects and assigns the ids to them...
    await service.insert(objects);
    console.timeEnd('create');

    // iterate over all the objects and mark them as running state (in parallel)
    console.time('update');
    let updateOpts = await Promise.all(objects.map(async (obj) => {
      obj.data.state = 'running';
      return service.update(obj);
    }));
    console.timeEnd('update');

  }
}

main().catch((err) => {
  setTimeout(() => {
    throw err;
  });
});
