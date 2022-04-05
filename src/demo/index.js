const express = require('express')
const app = express();
const port = 3000;
const { spawn } = require('child_process');
const fs = require('fs')
app.use(express.json());
process.chdir('./script');
const {Storage} = require('@google-cloud/storage');
const path = require('path');
const cwd = path.join(__dirname, '..');
var destFileName = path.join(cwd, '../src/demo/output/output.json')

// Creates a client
const storage = new Storage(
  {
    projectId: 'scala-project-344219',
    keyFilename: '../scala-project-344219-d0966018c429.json'
}
);
const BUCKET_NAME = 'scala-project-data-bucket'

app.get('/', (req, res) => {
  console.log(process.env.PATH)
  var run = spawn('./run.sh', [true, false, 'p', 3])
  //var run = spawn('spark-submit --class main ./target/scala-2.12/HelloWorldSpark-assembly-1.0.jar',[true, false, 'p', 2])
  run.stdout.on('data', data =>{
    console.log(`stdout:\n${data}`);
  })
});

app.get('/echo', (req, res) => {
  console.log("echo")
  var run = spawn('ls');
  run.stdout.on('data', data =>{
    console.log(`stdout:\n${data}`);
  })
  console.log("echo ended")
  res.sendStatus(200);
})


app.get('/createBucket', (req, res) => {
  const path = './01.gcs-make-bucket.sh';
  try {
    if (fs.existsSync(path)) {
      //file exists
      console.log("file exist")
      console.log("=========== CREAZIONE BUCKET AVVIATA. =============")
      var run = spawn(path)
      run.stdout.on('data', data =>{
        console.log(`stdout:\n${data}`);
      })
      run.on('close', () => {
        console.log("=========== CREAZIONE BUCKET TERMINATA. ===========")
        res.sendStatus(200);
      });
     
    }
  } catch(err) {
    res.sendStatus(404);
  }
 
})

app.get('/createCluster', (req, res) => {
  const path = './02.dataproc-create-cluster.sh'
  try {
    if (fs.existsSync(path)) {
      console.log("=========== CREAZIONE CLUSTER AVVIATA. ===========")

      var run = spawn(path)
      run.stdout.on('data', data =>{
        console.log(`stdout:\n${data}`);
      })
      run.on('close', () => {
        console.log("=========== CREAZIONE CLUSTER TERMINATA. ===========")
        res.sendStatus(200);
      });
     
    }
  } catch(err) {
    res.sendStatus(404);
  }
})

app.get('/deployCode', (req, res) => {
  const path = './03.deploy-code.sh'
  try {
    if (fs.existsSync(path)) {
      console.log("=========== DEPLOY CODE AVVIATO. ===========")
      var run = spawn(path)
      run.stdout.on('data', data =>{
        console.log(`stdout:\n${data}`);
      })
      run.on('close', () => {
        console.log("=========== DEPLOY CODE TERMINATO. ===========")
        res.sendStatus(200);
      });
    }
  } catch(err) {
    res.sendStatus(404);
  }
})

app.post('/runJob', (req, res) => {
  var PAR_MATRIX = req.body.parMatrix;
  var PAR_JOINING = req.body.parJoining;
  var METRIC = req.body.metric;
  var MAX_SEQUENCES_PER_FILE = req.body.maxSequencesPerFile;
  console.log("================= PARAMETERS ================")
  console.log("PAR_MATRIX = " + PAR_MATRIX)
  console.log("PAR_JOINING = " + PAR_JOINING)
  console.log("METRIC = " + METRIC)
  console.log("MAX_SEQUENCES_PER_FILE = " + MAX_SEQUENCES_PER_FILE)

  const path = './04.dataproc-run-job.sh';
  try {
    if (fs.existsSync(path)) {
      console.log("============= RUN JOB AVVIATO. =============")
      var run = spawn(path, [PAR_MATRIX, PAR_JOINING, METRIC, MAX_SEQUENCES_PER_FILE])
      run.stdout
        .on('data', data =>{
          console.log(`stdout:\n${data}`);
        })
      run.on('close', () => {
        console.log("============ RUN JOB TERMINATO. ============")
          downloadFile().then(res.sendFile(destFileName)).catch(console.error);
        });
      }
  } catch(err) {
    res.sendStatus(404);
  }
})

async function downloadFile() {
  const options = {
    destination: destFileName
  };

  // Downloads the file
  await storage.bucket(BUCKET_NAME).file('output.json').download(options);

  console.log(
    `gs://${BUCKET_NAME}/${'output.json'} downloaded to ${destFileName}.`
  );
}

app.listen(port, () => {
  console.log(`Example app listening on port ${port}!`)
});