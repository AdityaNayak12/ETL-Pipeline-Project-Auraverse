document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('etlForm');
  const input = document.getElementById('uploadInput');
  const loading = document.getElementById('loading');
  const output = document.getElementById('output');

  form.addEventListener('submit', ev => {
    ev.preventDefault();
    output.innerHTML = '';
    loading.style.display = 'block';

    let fd = new FormData();
    if (input.files.length) {
      fd.append('inputFile', input.files[0]);
    }

    fetch('http://localhost:5000/run-etl', {
      method: 'POST',
      body: fd,
    })
      .then(res => res.json())
      .then(data => {
        loading.style.display = 'none';
        if (data.success && data.table && data.table.length) {
          renderTableWithColumnFilter(data.table);
        } else {
          output.innerHTML = `<div style="color:#d02927;font-weight:bold;">Error: ${data.error || 'No data returned.'}</div>`;
        }
      })
      .catch(() => {
        loading.style.display = 'none';
        output.innerHTML =
          '<div style="color:#d02927;font-weight:bold;">Network or server error. Make sure backend is running.</div>';
      });
  });

  // Hides columns that are all empty across all rows
  function filterEmptyColumns(rows) {
    if (!rows || rows.length === 0) return {data: [], columns: []};
    const allKeys = Object.keys(rows[0]);
    const nonEmptyCols = allKeys.filter(
      key => rows.some(
        row => row[key] != null && row[key].toString().trim() !== "" && row[key] !== "null" && row[key] !== "undefined"
      )
    );
    const filteredData = rows.map(row => {
      const filteredRow = {};
      nonEmptyCols.forEach(col => filteredRow[col] = row[col]);
      return filteredRow;
    });
    return { data: filteredData, columns: nonEmptyCols };
  }

  function renderTableWithColumnFilter(rows) {
    const { data, columns } = filterEmptyColumns(rows);
    if (columns.length === 0) {
      output.innerHTML = "<p>No data to display after filtering empty columns.</p>";
      return;
    }
    let html = '<table><thead><tr>';
    html += columns.map(col => `<th>${col}</th>`).join('');
    html += '</tr></thead><tbody>';
    for (let row of data) {
      html += '<tr>' + columns.map(col =>
        `<td>${row[col] !== null && row[col] !== undefined ? row[col] : ''}</td>`).join('')
        + '</tr>';
    }
    html += '</tbody></table>';
    output.innerHTML = html;
  }
});
