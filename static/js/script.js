var project_name = MYLIBRARY.project_name();
var data_url = '/' + project_name + '/data/'
var refresh_time_interval = 10000

// generate timeline chart and barchart
$.getJSON(data_url,
    function(data_dict) {
        window.chart = c3.generate({
            bindto: '#timeline',
            data: {
                x: 'labels',
                columns: [
                    data_dict['timeline']['labels'],
                    data_dict['timeline']['Defocus'],
                    data_dict['timeline']['delta_Defocus'],
                    data_dict['timeline']['Phase_shift'],
                ],
                axes: {
                    delta_Defocus: 'y',
                    Defocus: 'y',
                    Phase_shift: 'y2'
                },
                types: {
                    delta_Defocus: 'bar',
                },
                groups: [
                    ['Defocus', 'delta_Defocus']
                ]
            },
            axis: {
                x: {
                    type: 'category',
                    extent: [0,50],
                    tick: {
                        fit: false
                    }
                },
                y: {
                    label: {
                        text: 'Difference defocus (\u03BCm)',
                        position: 'outer-middle'
                    }
                },
                y2: {
                    show: true,
                    label: {
                        text: 'Phase shift',
                        position: 'outer-middle'
                    }
                }
            },
            transition: {
                duration: 0
            },
            // set to false to speed things up
            interaction: {
                enabled: true
            },
            subchart: {
                show: true
            }
        });

        window.defocus = c3.generate({
            bindto: '#defocus',
            data: {
                x: 'bins',
                columns: [
                    data_dict['defocus']['bins'],
                    data_dict['defocus']['data']
                ],
                names: {
                    data: 'Defocus (\u03BCm)'
                },
                colors: {
                    data: '#7FFF00'
                },
                type: 'bar'
            },
            bar: {
                width: {
                    ratio: 0.95
                }
            }
        });
        window.resolution = c3.generate({
            bindto: '#resolution',
            data: {
                x: 'bins',
                columns: [
                    data_dict['resolution']['bins'],
                    data_dict['resolution']['data']
                ],
                names: {
                    data: 'Resolution (\u212B)'
                },
                colors: {
                    data: '#1E90FF'
                },
                type: 'bar'
            },
            bar: {
                width: {
                    ratio: 0.95
                }
            },
            axis: {
                x: {
                    tick: {
                        format: d3.format(",.2f")
                    }
                }
            }
        });
}); // end get JSON

//generate table data
$(document).ready(function() {
    var table = $('#example').DataTable( {
        "ajax": data_url,
        "columns": [
            { data: "Unnamed: 0" }, // this is the micrograph id
            { data: "Defocus_V" },
            { data: "Defocus_U" },
            { data: "Angle" },
            { data: "Phase_shift" },
            { data: "Resolution" }
        ],
        "rowCallback": function( row, data ) {
            $(row).attr('id', data["Unnamed: 0"]);
        }
    } );

    setInterval( function () {
        table.ajax.reload( null, false ); // user paging is not reset on reload
    }, refresh_time_interval );

    $('#example tbody').on( 'click', 'tr', function () {
        var mic_id = $(this).attr("id");
        $("#dw-image").attr("src",`/static/data/${project_name}/motioncor/${mic_id}_DW.png`);
        $("#ps-image").attr("src",`/static/data/${project_name}/gctf/${mic_id}.png`);

        if ( $(this).hasClass('selected') ) {
            $(this).removeClass('selected');
        }
        else {
            table.$('tr.selected').removeClass('selected');
            $(this).addClass('selected');
        }

    } );
} );


$(function() {
 update_progress(data_url); // url welche die daten als json zurückgibt
});

function update_progress(url) {
    $.getJSON(url,
        function(data_dict) {
        window.chart.load({
            columns: [
                data_dict['timeline']['labels'],
                data_dict['timeline']['Defocus'],
                data_dict['timeline']['delta_Defocus'],
                data_dict['timeline']['Phase_shift']
            ],
        });
        window.defocus.load({
            columns: [
                data_dict['defocus']['bins'],
                data_dict['defocus']['data']
            ],
        });
        window.resolution.load({
            columns: [
                data_dict['resolution']['bins'],
                data_dict['resolution']['data']
            ],
        });
        setTimeout(function () { update_progress(url); }, refresh_time_interval);
    });
};

