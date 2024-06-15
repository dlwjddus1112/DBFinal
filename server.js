const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const path = require('path');

const app = express();
const port = 3000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Static file serving
app.use(express.static(path.join(__dirname, 'public')));

const connection = mysql.createConnection({
  host: 'localhost',
  user: 'idwjddus',
  password: 'Dlwjddus123@',
  database: 'movieDB'
});

connection.connect(err => {
  if (err) {
    console.error('Error connecting to the database:', err);
    return;
  }
  console.log('Connected to the database');
});

app.get('/api/movies/search', (req, res) => {
  const { movieName, productionYearFrom, productionYearTo, directorName, sortOrder, page = 1 } = req.query;
  const itemsPerPage = 10;
  const offset = (page - 1) * itemsPerPage;

  let query = `
    SELECT 
      Movies.*,
      IFNULL(Movies.company, '') as productionCompany
    FROM Movies
    WHERE 1=1
  `;

  if (movieName) query += ` AND Movies.title LIKE '%${movieName}%'`;
  if (productionYearFrom) query += ` AND Movies.year >= ${productionYearFrom}`;
  if (productionYearTo) query += ` AND Movies.year <= ${productionYearTo}`;
  if (directorName) query += ` AND Movies.director LIKE '%${directorName}%'`;

  if (sortOrder === 'Movies.title') {
    query += ` AND Movies.title IS NOT NULL AND Movies.title != ''`;
  }

  const countQuery = `SELECT COUNT(*) as count FROM (${query}) as total`;

  query += ` ORDER BY ${sortOrder || 'Movies.enter_date DESC'} LIMIT ${itemsPerPage} OFFSET ${offset}`;
  console.log("Executing query: ", query);

  connection.query(countQuery, (countError, countResults) => {
    if (countError) {
      console.error('Error executing count query:', countError.message);
      res.status(500).send('Internal Server Error');
      return;
    }

    const totalItems = countResults[0].count;
    const totalPages = Math.ceil(totalItems / itemsPerPage);

    connection.query(query, (error, results) => {
      if (error) {
        console.error('Error executing query:', error.message);
        res.status(500).send('Internal Server Error');
        return;
      }
      res.json({
        movies: results,
        totalItems: totalItems,
        totalPages: totalPages,
        currentPage: parseInt(page, 10)
      });
    });
  });
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
