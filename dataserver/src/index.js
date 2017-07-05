import * as d3 from 'd3';
import {Chart} from 'chart';

let chart = new Chart('svg');

let filter = {
  categories: ['champion', 'contender', 'challenger'],
  divYield: {
    min: 0,
    max: 1000
  },
  payout: {
    min: 0,
    max: 300000
  },
  relative: {
    min: -1000,
    max: 1000
  },
  divg3y: {
    min: 0,
    max: 1000
  },
  yieldGrowthSum: {
    min: 0,
    max: 1000
  },
  outliers: false
}

d3.json('/data/companies', data => {
  chart.setData(preprocess(data, filter)).setFilter(filter).update();

  let links = document.getElementsByClassName('switch');
  Array.prototype.forEach.call(links, link => {
    link.addEventListener('click', e => {
      let axis = e.target.attributes.axis.value;
      let field = e.target.attributes.field.value;
      let label = e.target.innerText;
      chart.set(axis, field, label).update();        
    });
  });

  let categoryCheckBoxes = document.getElementsByClassName('category-filter');
  Array.prototype.forEach.call(categoryCheckBoxes, checkbox => {
    let category = checkbox.nextSibling.textContent.trim().toLowerCase();
    let index = filter.categories.findIndex(item => item === category);

    checkbox.checked = index !== -1;
    checkbox.addEventListener('change', e => {
      let index = filter.categories.findIndex(item => item === category);
      if (e.target.checked && index === -1) {
        filter.categories.push(category);
      } else if (!e.target.checked && index!== -1) {
        filter.categories.splice(index, 1);
      }
      chart.setFilter(filter).update();
    });
  });

  let outlier = document.getElementById('outlier-filter');
  outlier.checked = filter.outliers;
  outlier.addEventListener('change', e => {
    filter.outliers = e.target.checked;
    chart.setData(preprocess(data, filter)).update()
  });

  let form = document.getElementById('filterForm');
  Array.prototype.forEach.call(form, elem => {
    if (elem.type === 'number') {
      elem.addEventListener('change', e => {
        let id = e.target.id.split('.');
        let value = e.target.value;
        filter[id[0]][id[1]] = value;
        chart.setFilter(filter).update();
      });      
    }
  });
});

function preprocess(data, filter) {
  let cleanedData = data;

  if (filter.outliers) {
    data.sort((a, b) => a.payout - b.payout);
    let payoutTreshold = d3.quantile(data, 0.95, d => d.payout);

    data.sort((a, b) => a.divg3y - b.divg3y);
    let growthTreshold = d3.quantile(data, 0.95, d => d.divg3y);

    cleanedData = data.filter(item => !!item.payout && item.payout < payoutTreshold && item.divg3y < growthTreshold);
  }
  
  return cleanedData
  .map(item => {
    item.relative = (item.divYield - item.yieldDist5y.mean) / item.yieldDist5y.std;
    return item;
  });
}
