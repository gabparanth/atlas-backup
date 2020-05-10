const request = require("request");
const util = require("util");
const request_promise = require("request-promise");
const async = require("async");
const { exec } = require("child_process");
var config = require("./config");

const public_key = config.public_key;
const private_key = config.private_key;
const group_id = config.group_id;
const db_username = config.db_username;
const db_password = config.db_password;

const instance_tiers = "M50";
const interval = 10000; // 10s

const source_cluster = "Prod"; // prod cluster
const cluster_name = "temp"; // temp cluster
const test_cluster = "test"; // test cluster

const db_name = "test-backup";

var snapshotId = null;

async function create_snapshot(cluster_name) {
  var options = {
    method: "POST",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      cluster_name +
      "/backup/snapshots",
    headers: { "content-type": "application/json" },
    body: { description: "SomeDescription", retentionInDays: 5 },
    json: true,
  };

  try {
    const parsedBody = await request_promise(options).auth(
      public_key,
      private_key,
      false
    );

    var snapshot_id = JSON.stringify(parsedBody.id);

    console.log("create snapshot on " + cluster_name + " - id: " + snapshot_id);

    return Promise.resolve(snapshot_id);
  } catch (error) {
    return Promise.reject(error);
  }

  /// SAVE CODE
  // request_promise(options)
  //   .auth(public_key, private_key, false)
  //   .then(function (parsedBody) {
  //     // POST succeeded...
  //     snapshot_id = JSON.stringify(parsedBody.id);
  //     console.log(
  //       "create snapshot on " +
  //         cluster_name +
  //         " - id: " +
  //         JSON.stringify(parsedBody.id)
  //     );
  //   })
  //   .catch(function (err) {
  //     console.log(err);
  //     // POST failed...
  //   });
}

async function get_snapshot_status(cluster_name, snapshotId) {
  var options = {
    method: "GET",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      cluster_name +
      "/backup/snapshots",
  };

  try {
    const parsedBody = await request_promise(options).auth(
      public_key,
      private_key,
      false
    );
    // console.log(snapshotId);

    var results = JSON.parse(parsedBody).results;
    // console.log("results : " + JSON.stringify(results));
    // console.log("snapshotId : " + snapshotId);
    for (var doc in results) {
      // console.log("results[doc].id - " + results[doc].id);
      // console.log("snapshotId - " + snapshotId);
      // console.log(
      //   "debug : JSON.stringify(results[doc].id) : " +
      //     JSON.stringify(results[doc].id) +
      //     "|  snapshotId : " +
      //     snapshotId
      // );
      if (JSON.stringify(results[doc].id) == snapshotId) {
        // console.log("results[doc].id - " + results[doc].id);
        // console.log("snapshotId - " + snapshotId);

        var status = JSON.stringify(results[doc].status);
        // console.log("status : " + status);
        return Promise.resolve(status);
      }
    }
  } catch (error) {
    return Promise.reject(error);
  }
}

async function create_cluster(name, instance_tiers, callback) {
  var body_create_cluster = {
    name: name,
    diskSizeGB: 2100,
    numShards: 1,
    providerSettings: {
      providerName: "GCP",
      instanceSizeName: instance_tiers,
      regionName: "EUROPE_WEST_2",
    },
    clusterType: "REPLICASET",
    replicationFactor: 3,
    replicationSpecs: [
      {
        numShards: 1,
        regionsConfig: {
          EUROPE_WEST_2: {
            analyticsNodes: 0,
            electableNodes: 3,
            priority: 7,
            readOnlyNodes: 0,
          },
        },
        zoneName: "Zone 1",
      },
    ],
    backupEnabled: false,
    providerBackupEnabled: true,
    autoScaling: { diskGBEnabled: true },
  };

  var options = {
    method: "POST",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters",
    headers: { "content-type": "application/json" },
    body: body_create_cluster,
    json: true,
  };

  request_promise(options)
    .auth(public_key, private_key, false)
    .then(function (parsedBody) {
      console.log("create cluster " + name + " in progress ");
    })
    .catch(function (err) {
      console.log(err);
      // POST failed...
    });
}

function get_cluster_status(cluster_name, callback) {
  var options = {
    method: "GET",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      cluster_name,
  };

  body = [];

  return request
    .get(options)
    .auth(public_key, private_key, false)
    .on("data", function (chunk) {
      body.push(chunk);
    })
    .on("end", function () {
      body = Buffer.concat(body).toString();
      callback(JSON.parse(body).stateName);
      // return JSON.parse(body).stateName;
    })
    .on("error", function (error) {
      callback(error);
      // return error;
    });
}

async function create_restore_job(
  snapshotId,
  sourceCluster,
  targetClusterName,
  group_id
) {
  var options = {
    method: "POST",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      sourceCluster +
      "/backup/restoreJobs",
    headers: { "content-type": "application/json" },
    body: {
      snapshotId: snapshotId,
      deliveryType: "automated",
      targetClusterName: targetClusterName,
      targetGroupId: group_id,
    },
    json: true,
  };

  var parsedBody = await request_promise(options).auth(
    public_key,
    private_key,
    false
  );
  // console.log("creating restore job: " + JSON.parse(parsedBody).id);
  // return { restore_job_id: JSON.parse(parsedBody).id };
  console.log("creating restore job: " + parsedBody.id);
  return Promise.resolve(parsedBody.id);
}

async function get_restore_job_status(snapshotId, clusterName, group_id) {
  var options = {
    method: "GET",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      clusterName +
      "/backup/restoreJobs/" +
      snapshotId,
  };
  var restore_job_status = null;

  var parsedBody = await request_promise(options).auth(
    public_key,
    private_key,
    false
  );
  console.log("debug parsedBody: " + JSON.parse(parsedBody));

  if (typeof JSON.parse(parsedBody).finishedAt == "undefined") {
    restore_job_status = false;
    return Promise.resolve(restore_job_status);
  } else {
    restore_job_status = true;
    return Promise.resolve(restore_job_status);
  }
}

async function get_cluster_uri(clusterName) {
  var options = {
    method: "GET",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      clusterName,
  };

  var parsedBody = await request_promise(options).auth(
    public_key,
    private_key,
    false
  );

  // ("mongodb://test-shard-00-00.uip7x.gcp.mongodb.net:27017,test-shard-00-01.uip7x.gcp.mongodb.net:27017,test-shard-00-02.uip7x.gcp.mongodb.net:27017/?ssl=true&authSource=admin&replicaSet=atlas-1382u3-shard-0");

  var mongoURIWithOptions = JSON.parse(parsedBody).mongoURIWithOptions;
  var mongoURI = JSON.parse(parsedBody).mongoURI;

  const regex = /[^=]+$/gm;

  var hostname = mongoURIWithOptions.match(regex)[0];

  console.log("HOSTMAME " + hostname);

  var atlas_host = mongoURI.replace("mongodb:/", hostname);

  console.log("-------- atlas_host --------------- " + atlas_host);

  return atlas_host;
}

async function remove_cluster(clusterName) {
  var options = {
    method: "DELETE",
    url:
      "https://cloud.mongodb.com/api/atlas/v1.0/groups/" +
      group_id +
      "/clusters/" +
      clusterName,
  };

  var parsedBody = await request_promise(options).auth(
    public_key,
    private_key,
    false
  );
  console.log("cluster " + clusterName + " is beeing removed");
  return;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function snapshot_task() {
  var snapshot_status = "null";
  var id = await create_snapshot(source_cluster);

  // TODO: clean this.
  const regex = /"/gi;

  // console.log(p.replace(regex, ''));

  snapshotId = id.replace(regex, "");

  console.log("snapshotId: " + snapshotId);
  // TODO : fix get_snapshot_status(source_cluster, id) returned value to no have double quote
  while (snapshot_status.trim() != '"completed"') {
    await sleep(interval); //10s
    snapshot_status = await get_snapshot_status(source_cluster, id);
  }

  console.log("snapshot task done");
}

async function cluster_creation_task() {
  var cluster_status = null;

  create_cluster(cluster_name, instance_tiers);

  while (cluster_status != "IDLE") {
    await sleep(interval);
    get_cluster_status(cluster_name, function (res) {
      cluster_status = res;
      console.log(cluster_status);
    });
  }

  console.log("cluster_creation_task complete");
}

async function job_restore_task() {
  var restore_job_status = null;
  var restore_job_id = await create_restore_job(
    snapshotId,
    source_cluster,
    cluster_name,
    group_id
  );

  console.log("debug restore_job_id: " + restore_job_id);

  while (restore_job_status != true) {
    await sleep(interval);
    restore_job_status = await get_restore_job_status(
      restore_job_id,
      source_cluster,
      group_id
    );
    console.log(restore_job_status);
  }
}

async function dump_restore_task(dump_cluster, restore_cluster) {
  var cmd_dump =
    "mongodump --host " +
    (await get_cluster_uri(dump_cluster)) +
    " --ssl --username " +
    db_username +
    " --password " +
    db_password +
    " --authenticationDatabase admin --db " +
    db_name +
    " --archive | mongorestore --drop --host " + // -drop to dump existing db
    (await get_cluster_uri(restore_cluster)) +
    " --ssl --username " +
    db_username +
    " --password " +
    db_password +
    " --authenticationDatabase admin --archive";

  console.log(" ************   " + cmd_dump + "  *************");

  await exec(cmd_dump, (error, stdout, stderr) => {
    if (error) {
      console.log(`error: ${error.message}`);
      // return;
    }
    if (stderr) {
      console.log(`stderr: ${stderr}`);
      // return;
    }
    console.log(`stdout: ${stdout}`);
  }).on("close", (code) => {
    console.log(`child process exited with code ${code}`);
  });
}

async function app() {
  let promises = [];

  var startTime = new Date();
  console.log("process starting at : " + startTime);
  promises.push(snapshot_task());
  promises.push(cluster_creation_task());

  Promise.all(promises)
    .then(() => {
      return job_restore_task();
    })
    .then(() => {
      return dump_restore_task(cluster_name, test_cluster);
    })
    .then(() => {
      // console.log("process ending at : " + endTime);
      // console.log(
      //   "process executed in : " +
      //     ~~((endTime - startTime) / 1000 / 60) +
      //     " min and " +
      //     (((endTime - startTime) / 1000) % 60) +
      //     "seconds"
      // );
      // return remove_cluster(cluster_name);
    });

  Promise.resolve();
  // var endTime = new Date();
  // console.log("END : " + endTime);
}

app();
