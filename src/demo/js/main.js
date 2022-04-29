$table.bootstrapTable('removeAll')
$table.bootstrapTable('load', getJsonData())
$table.bootstrapTable()
$table.bootstrapTable('load', getJsonData())

// Handle click row
// 1. show relative graph
// 2. update UI
$table.on('click-row.bs.table', function (e, row, $element) {
    svg_container.removeChild(svg_container.lastChild)
    var row_num = $element.index();
    
    chart = ForceGraph(dataJson[row_num], {
        nodeId: d => d.id,
        nodeGroup: d => d.group,
        nodeTitle: d => `${d.id} - ${d.group}`,
        linkStrokeDistance: l => l.value,
        width:700,
        height: 400,
    })

    showGrapg(chart)
    updateUI(parseJSON(dataJson[row_num]))   
});

