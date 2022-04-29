var $table = $(".table"); 
var $tbody = $(".tbody"); 
var $button = $("#button"); 
var svg_container = document.getElementById("svg-container")
var $file_name = document.getElementById("file_name");
var $seq_file = document.getElementById("seq_file");
var $input_dim = document.getElementById("input_file");
var $metric = document.getElementById("metric");
var $par_dist = document.getElementById("par_dist");
var $par_join = document.getElementById("par_join");
var $dist_time = document.getElementById("dist_time");
var $nj_time = document.getElementById("nj_time");
var $time = document.getElementById("total_time");

function updateUI(parsedJSON){
    $file_name.innerHTML = parsedJSON.file_name,
    $seq_file.innerHTML = parsedJSON.seq_file,
    $input_dim.innerHTML = parsedJSON.input_dim,
    $metric.innerHTML = parsedJSON.metric,
    $par_dist.innerHTML = parsedJSON.par_dist,
    $par_join.innerHTML = parsedJSON.par_join,
    $dist_time.innerHTML = parsedJSON.dist_time,
    $nj_time.innerHTML = parsedJSON.nj_time,
    $time.innerHTML = parsedJSON.time
}


function showGrapg(chart){
    var div = document.createElement('div');
    div.appendChild(chart)
    svg_container.append(div)
}