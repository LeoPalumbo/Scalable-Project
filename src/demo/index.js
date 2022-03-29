const express = require('express')
const app = express();
const port = 3000;
const { spawn } = require('child_process');


app.get('/', (req, res) => {
  console.log(process.env.PATH)
  var run = spawn('./run.sh', [true, false, 'p', 3])
  //var run = spawn('spark-submit --class main ./target/scala-2.12/HelloWorldSpark-assembly-1.0.jar',[true, false, 'p', 2])
  run.stdout.on('data', data =>{
    console.log(`stdout:\n${data}`);
  })
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}!`)
});