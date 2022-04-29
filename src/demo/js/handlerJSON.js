const FILE_DIM_1_SEQ = 30.2
const FILE_DIM_2_SEQ = 60.6

function parseJSON(json) {
    var file_name = json["name"]
    var seq_file = json["nodes"].filter(({group}) => group !== 'extra').length;
    var input_dim = (seq_file == 8) ? FILE_DIM_1_SEQ*seq_file+" kb" : FILE_DIM_2_SEQ*seq_file+" kb" 
    var metric = json["metric"]
    var par_dist = json["par_distance"] ? "TRUE" : "FALSE" 
    var par_join = json["par_joining"] ? "TRUE" : "FALSE" 
    var dist_time = ms2min(json["dist_time"])
    var nj_time =  ms2min(json["nj_time"])
    var time = ms2min(json["dist_time"] + json["nj_time"])

    return {
        file_name,
        seq_file,
        input_dim,
        metric,
        par_dist,
        par_join,
        dist_time,
        nj_time,
        time
    }
}


function getJsonData(){
    var rows = []

    dataJson.forEach(e => {
        var parsedJSON = parseJSON(e)

        rows.push({
            file_name: parsedJSON.file_name,
            seq_file: parsedJSON.seq_file,
            input_dim: parsedJSON.input_dim,
            metric: parsedJSON.metric,
            par_dist: parsedJSON.par_dist,
            par_join: parsedJSON.par_join,
            time: parsedJSON.time
        })
    })
    return rows 
}