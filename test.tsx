import React from 'react';

const FrenchDataCsv = () => {
  const frenchData = [
    { id: 1, name: 'Élodie', age: 30 },
    { id: 2, name: 'François', age: 28 },
    { id: 3, name: 'Renée', age: 35 },
    // Add more French data as needed
  ];

  const downloadCsv = () => {
    const csvContent = "\uFEFF" + convertArrayOfObjectsToCSV(frenchData);
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', 'french_data.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const convertArrayOfObjectsToCSV = (data) => {
    const csv = data.map(row => Object.values(row).map(value => `"${value}"`).join(',')).join('\n');
    return `id,name,age\n${csv}`; // Assuming CSV header with columns id, name, age
  };

  return (
    <div>
      <h1>French Data CSV Example</h1>
      <button onClick={downloadCsv}>Download CSV</button>
    </div>
  );
};

export default FrenchDataCsv;
