function buildMetadata(sample) {
  d3.json(`/metadata/${sample}`).then((data) => {
    // Use d3 to select the panel with id of `#sample-metadata`
    var PANEL = d3.select("#sample-metadata");

    // Use `.html("") to clear any existing metadata
    PANEL.html("");

    // Use `Object.entries` to add each key and value pair to the panel
    // Hint: Inside the loop, you will need to use d3 to append new
    // tags for each key-value in the metadata.
    Object.entries(data).forEach(([key, value]) => {
      PANEL.append("h6").text(`${key}: ${value}`);
    });

    // BONUS: Build the Gauge Chart
    buildGauge(data['Diagnosis Count']);
  });
}

function buildCharts(sample) {
  d3.json(`/samples/${sample}`).then((data) => {
    const otu_labels = data.otu_labels;
    const sample_values = data.sample_values;
    console.log(otu_labels)
    console.log(sample_values)
    // Build a Pie Chart
    var pieData = [
      {
        values: sample_values.slice(0, 10),
        labels: otu_labels.slice(0, 10),
        textposition:'inside',
        textinfo:'percent',
        hoverinfo: 'label+percent',
        textfont: {
          family: 'Lato',
          color: 'white',
          size: 12
        },
        hoverlabel: {
          bgcolor: 'black',
          bordercolor: 'black',
          font: {
            family: 'Lato',
            color: 'white',
            size: 12
          }
        },
        type: "pie"
      }
    ];

    var pieLayout = {
      height: 400,
      width: 500,
      margin: { t: 0, l: 0 },
      showlegend: false
    };

    Plotly.plot("pie", pieData, pieLayout);
  });
}

function init() {
  // Grab a reference to the dropdown select element
  var selector = d3.select("#selDataset");

  // Use the list of sample names to populate the select options
  d3.json("/names").then((sampleNames) => {
    sampleNames.forEach((sample) => {
      selector
        .append("option")
        .text(sample)
        .property("value", sample);
    });

    // Use the first sample from the list to build the initial plots
    const firstSample = sampleNames[0];
    buildCharts(firstSample);
    buildMetadata(firstSample);
  });
}

function optionChanged(newSample) {
  // Fetch new data each time a new sample is selected
  buildCharts(newSample);
  buildMetadata(newSample);
}

// Initialize the dashboard
init();
