import * as d3 from 'd3';

const MARGIN = {
  left: 60,
  right: 60,
  top: 60,
  bottom: 60
};
const WIDTH = 700 - MARGIN.left - MARGIN.right;
const HEIGHT = 500 - MARGIN.top - MARGIN.bottom;
const MIN_DIMENSION = Math.min(WIDTH, HEIGHT);
const ease = d3.easeCubicOut;
const slow = 600;
const fast = 200;

export class Chart {
  constructor(selector, data) {
    this.selector = selector;
    this.data = data || [];
    
    // Starter config
    this.config = {};
    this.set('x', 'divg3y', '3 years dividend growth');
    this.set('y', 'divYield', 'Dividend yield');
    this.set('color', undefined);
    this.set('r', undefined);

    // Basic chart elements
    this.svg = d3.select(selector)
    .attr('width', WIDTH + MARGIN.left + MARGIN.right)
    .attr('height', HEIGHT + MARGIN.bottom + MARGIN.top)
    .append('g')
    .attr('width', WIDTH)
    .attr('height', HEIGHT)
    .attr('transform', `translate(${MARGIN.left}, ${MARGIN.top})`);

    // Clickable background
    this.clickArea = d3.select(selector)
    .append('rect')
    .lower()
    .attr('id', 'clickArea')
    .attr('width', WIDTH + MARGIN.left + MARGIN.right)
    .attr('height', HEIGHT + MARGIN.top + MARGIN.bottom)
    .style('fill', 'transparent')
    .call((selection, config, context) => {
      selection.on('click', function(d) {
        let persists = d3.selectAll('.persist').classed('persist', false);
        context.removeHighLight(persists, d, context.config);
        context.persist = null;
        document.getElementById('infopanel').innerHTML = '';
      })
    }, this.config, this);

    this.xAxis = this.svg.append('g').attr('class', 'axis').attr('transform', `translate(0, ${HEIGHT})`);
    this.yAxis = this.svg.append('g').attr('class', 'axis');

    this.xLabel = this.svg.append('text')
    .text(this.config.xLabel)
    .attr('class', 'axis-label')
    .attr('text-anchor', 'end')
    .attr('transform', `translate(${WIDTH + 10}, ${HEIGHT + MARGIN.bottom - 20})`);

    this.yLabel = this.svg.append('text')
    .text(this.config.yLabel)
    .attr('class', 'axis-label')
    .attr('text-anchor', 'start')
    .attr('transform', `translate(-20, -20)`);
  }

  setData(data) {
    this.data = data;
    this.recalibrate();
    return this;
  }

  setFilter(filter) {
    this.filter = filter;
    return this;
  }

  getData() {
    if (this.filter) {      
      return this.data
      .filter(item => this.filter.categories.find(category => item.category === category))
      .filter(item => item.divYield >= this.filter.divYield.min && item.divYield < this.filter.divYield.max)
      .filter(item => item.payout >= this.filter.payout.min && item.payout < this.filter.payout.max)    
      .filter(item => item.relative >= this.filter.relative.min && item.relative < this.filter.relative.max)
      .filter(item => item.divg3y >= this.filter.divg3y.min && item.divg3y < this.filter.divg3y.max)
      .filter(item => item.divg3y + item.divYield >= this.filter.yieldGrowthSum.min)
    } else {
      return this.data;
    }
  }

  recalibrate() {
    this.set('x', this.config.x, this.xLabel.text());
    this.set('y', this.config.y, this.yLabel.text());
    this.set('color', this.config.color);
    this.set('r', this.config.radius);
    return this;
  }

  set(axis, field, label) {
    axis = axis.toLowerCase();

    if (axis === 'x' || axis === 'y') {
      this.config[axis] = field || this.config[axis];
      this.config[`${axis}Scale`] = this.createLinearScale(this.config[axis], axis);
      this.config[`${axis}Axis`] = this.createAxis(this.config[`${axis}Scale`], axis);
      this.config[`${axis}Label`] = label || field;        
    } else if (axis === 'color') {
      this.config.color = field;
      this.config.colorScale = this.createColorScale(this.config.color);
    } else if (axis === 'r') {
      this.config.radius = field;
      this.config.rScale = this.createRadiusScale(this.config.radius);
    } else {
      throw Error('Invalid axis');
    }
    return this;
  }

  createLinearScale(field, axis) {
    let minValue = d3.min(this.data, d => d[field]);
    let maxValue = d3.max(this.data, d => d[field]);
    if (axis.toLowerCase() === 'x') {
      return d3.scaleLinear().domain([minValue, maxValue]).range([0, WIDTH]).nice()
    } else {
      return d3.scaleLinear().domain([minValue, maxValue]).range([HEIGHT, 0]).nice() 
    }
  }

  createAxis(scale, axis) {
    if (axis.toLowerCase() === 'x') {
      return d3.axisBottom(scale).tickSize(0);
    } else {
      return d3.axisLeft(scale).tickSize(0);
    }
  }

  createColorScale(field) {
    if (field) {
      let sortedData = this.data.sort((a, b) => a[field] - b[field]);
      let median = d3.quantile(sortedData, 0.5, d => d[field]);
      let topQuartile = d3.quantile(sortedData, 0.75, d => d[field]);    
      return d3.scaleLinear().domain([0, median, topQuartile]).range(['green', 'yellow', 'red']);        
    } else {
      return function(d) {return '#660B60'};
    }
  }

  createRadiusScale(field) {
    if (field) {
      return d3.scaleLinear().domain(d3.extent(this.data, d => d[field])).range([3, 12]);
    } else {
      return function(d) {return 5};        
    }
  }

  update() {
    // Join data
    let dots = this.svg.selectAll('.bubble').data(this.getData(), d => d.ticker);

    // Enter
    dots.enter()
    .append('g')
    .attr('class', 'bubble')
    .attr('id', d => d.ticker)
    .attr('transform', d => {
      return `translate(${this.config.xScale(d[this.config.x])}, ${this.config.yScale(d[this.config.y])})`
    })
    .append('circle')
    .style('fill', d =>  this.config.colorScale(d[this.config.color]))
    .style('opacity', 0)
    .attr('r', 0)
    .transition()
    .ease(ease)
    .duration(slow)
    .style('opacity', 0.8)
    .attr('r', d => this.config.rScale(d[this.config.radius]))
    .selection()
    .call(this.attachListeners, this.config, this);
    
    // Update
    dots
    .transition()
    .ease(ease)
    .duration(slow)
    .attr('transform', d => {
      return `translate(${this.config.xScale(d[this.config.x])}, ${this.config.yScale(d[this.config.y])})`
    })
    .select('circle')
    .attr('r', d => {
      if (this.persist === d.ticker) {
        return 25;
      } else {
        return this.config.rScale(d[this.config.radius]);
      }
    })
    .style('fill', d =>  this.config.colorScale(d[this.config.color]));

    // Exit
    dots.exit()
    .transition()
    .ease(ease)
    .duration(slow)
    .style('opacity', 0)
    .remove()

    // Axis and labels
    if (this.xLabel.text() !== this.config.xLabel) {
      this.xLabel
      .transition()
      .ease(ease)
      .duration(fast)
      .style('opacity', 0)
      .attr('x', 200)
      .transition()
      .ease(ease)
      .duration(slow)
      .text(this.config.xLabel)
      .attr('x', 0)
      .style('opacity', 1);      
    }

    if (this.yLabel.text() !== this.config.yLabel) {
      this.yLabel
      .transition()
      .ease(ease)
      .duration(fast)
      .style('opacity', 0)
      .attr('y', -200)
      .transition()
      .ease(ease)
      .duration(slow)
      .text(this.config.yLabel)
      .attr('y', 0)
      .style('opacity', 1);
    }

    this.xAxis
    .transition()
    .ease(ease)
    .duration(slow)
    .attr('transform', `translate(0, ${this.config.yScale(Math.max(0, this.config.yScale.domain()[0]))})`)
    .call(this.config.xAxis);
    
    this.yAxis
    .transition()
    .ease(ease)
    .duration(slow)
    .call(this.config.yAxis);
  }

  attachListeners(selection, config, context) {
    selection.on('mouseover', function(d) {
      if (context.persist !== d.ticker) {
        context.highlight(d3.select(this.parentNode), d);
      }
    });

    selection.on('mouseout', function(d) {
      if (context.persist !== d.ticker) {      
        context.removeHighLight(d3.select(this.parentNode), d, context.config);
      }
    });

    selection.on('click', function(d) {
      console.log(d);

      if (context.persist !== d.ticker) {
        let persists = d3.selectAll('.persist').classed('persist', false);
        context.removeHighLight(persists, d, context.config);
        d3.select(this.parentNode).classed('persist', true);
        context.persist = d.ticker;
      }

      let xhr = new XMLHttpRequest();
      xhr.open('GET', `/info/${d.ticker}`)
      xhr.onload = function(e) {
        if (xhr.readyState === 4 && xhr.status === 200) {
          document.getElementById('infopanel').innerHTML = xhr.responseText;
        }
      }
      xhr.send();
    });
  }

  highlight(selection, d) {
    selection.raise()
    .append('text')
    .attr('transform', 'translate(0,2)')
    .text(d.ticker)
    .style('opacity', 0)
    .classed('ticker', true)
    .transition()
    .ease(ease)
    .duration(slow)
    .style('opacity', 1);

    selection
    .select('circle')
    .transition()
    .ease(ease)
    .duration(fast)
    .attr('r', 25)
    .style('opacity', 1)
    .style('stroke', 'white')
    .style('fill', '#660B60')
    .style('stroke-width', 2);
  }

  removeHighLight(selection, d, config) {
    selection.select('.ticker')
    .transition()
    .ease(ease)
    .duration(fast)
    .style('opacity', 0)
    .remove();

    selection.select('circle')
    .transition()
    .ease(ease)
    .duration(fast)
    .attr('r', d => config.rScale(d[config.radius]))
    .style('fill', d => config.colorScale(d[config.color]))
    .style('opacity', 0.8)
    .style('stroke-width', 0);

    selection.lower()
  }
}