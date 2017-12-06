function variance (x) {
  var n = x.length;
  if (n < 1) return NaN;
  if (n === 1) return 0;
  var mean = d3.mean(x),
      i = -1,
      s = 0;
  while (++i < n) {
    var v = x[i] - mean;
    s += v * v;
  }
  return s / (n - 1);
}
//A test for outliers http://en.wikipedia.org/wiki/Chauvenet%27s_criterion
function chauvenet (x) {
    var dMax = 2;
    var mean = d3.mean(x);
    var stdv = Math.sqrt(variance(x));
    var counter = 0;
    var temp = [];

    for (var i = 0; i < x.length; i++) {
        if(dMax > (Math.abs(x[i] - mean))/stdv) {
            temp[counter] = x[i];
            counter = counter + 1;
        }
    }
    return temp
}

function histogram (data_input, color, id) {

        var trimmed = chauvenet(data_input);

        // var color = "Olive";

        // Generate a 1000 data points using normal distribution with mean=20, deviation=5
        //var values = d3.range(1000).map(d3.random.normal(20, 5));

        // A formatter for counts.
        var formatCount = d3.format(",.0f");

        var margin = {top: 20, right: 30, bottom: 30, left: 30},
            width = 960 - margin.left - margin.right,
            height = 500 - margin.top - margin.bottom;

        var max = d3.max(trimmed);
        var min = d3.min(trimmed);

        var x = d3.scale.linear()
              .domain([min, max])
              .range([0, width]);

        // Generate a histogram using twenty uniformly-spaced bins.
        var data = d3.layout.histogram()
            .bins(x.ticks(10))
            (trimmed);

        var yMax = d3.max(data, function(d){return d.length});
        var yMin = d3.min(data, function(d){return d.length});
        var colorScale = d3.scale.linear()
                    .domain([yMin, yMax])
                    .range([d3.rgb(color).brighter(), d3.rgb(color).darker()]);

        var y = d3.scale.linear()
            .domain([0, yMax])
            .range([height, 0]);

        var xAxis = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var svg = d3.select(id)
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
          .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        var bar = svg.selectAll(".bar")
            .data(data)
          .enter().append("g")
            .attr("class", "bar")
            .attr("transform", function(d) { return "translate(" + x(d.x) + "," + y(d.y) + ")"; });

        bar.append("rect")
            .attr("x", 1)
            .attr("width", (x(data[0].dx) - x(0)) - 1)
            .attr("height", function(d) { return height - y(d.y); })
            .attr("fill", function(d) { return colorScale(d.y) });

        bar.append("text")
            .attr("dy", ".75em")
            .attr("y", -12)
            .attr("x", (x(data[0].dx) - x(0)) / 2)
            .attr("text-anchor", "middle")
            .text(function(d) { return formatCount(d.y); });

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        /*
        * Adding refresh method to reload new data
        */
        function refresh(values){
          // var values = d3.range(1000).map(d3.random.normal(20, 5));
          var data = d3.layout.histogram()
            .bins(x.ticks(20))
            (values);

          // Reset y domain using new data
          var yMax = d3.max(data, function(d){return d.length});
          var yMin = d3.min(data, function(d){return d.length});
          y.domain([0, yMax]);
          var colorScale = d3.scale.linear()
                      .domain([yMin, yMax])
                      .range([d3.rgb(color).brighter(), d3.rgb(color).darker()]);

          var bar = svg.selectAll(".bar").data(data);

          // Remove object with data
          bar.exit().remove();

          bar.transition()
            .duration(1000)
            .attr("transform", function(d) { return "translate(" + x(d.x) + "," + y(d.y) + ")"; });

          bar.select("rect")
              .transition()
              .duration(1000)
              .attr("height", function(d) { return height - y(d.y); })
              .attr("fill", function(d) { return colorScale(d.y) });

          bar.select("text")
              .transition()
              .duration(1000)
              .text(function(d) { return formatCount(d.y); });

        }

        // Calling refresh repeatedly.
        // setInterval(function() {
        //   var values = d3.range(1000).map(d3.random.normal(20, 5));
        //   refresh(values);
        // }, 2000);

        // http://bl.ocks.org/phil-pedruco/6917114
        // Borrowed from Jason Davies science library https://github.com/jasondavies/science.js/blob/master/science.v1.js


}