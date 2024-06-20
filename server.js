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
    SELECT m.movie_id, m.title, m.eng_title, m.year, m.country, m.m_type, m.status, m.company, g.genre_name AS genre, md.director_ids
    FROM movies m
    LEFT JOIN movie_director md ON m.movie_id = md.movie_id
    
    LEFT JOIN genres g ON m.movie_id = g.movie_id
    LEFT JOIN directors d ON md.director_ids = d.director_id
    
    WHERE 1=1
  `;

  if (movieName) query += ` AND m.title LIKE '%${movieName}%'`;
  if (productionYearFrom) query += ` AND m.year >= ${productionYearFrom}`;
  if (productionYearTo) query += ` AND m.year <= ${productionYearTo}`;
  if (directorName) query += ` AND d.director_name LIKE '%${directorName}%'`;

  const countQuery = `SELECT COUNT(*) as count FROM (${query}) as total`;

  if (sortOrder && sortOrder !== '--선택--') {
    query += ` ORDER BY ${sortOrder}`;
  } else {
    query += ` ORDER BY m.movie_id ASC`;
  }

  query += ` LIMIT ${itemsPerPage} OFFSET ${offset}`;

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

      const moviePromises = results.map(movie => {
        return new Promise((resolve, reject) => {
          if (movie.director_ids) {
            const directorIds = movie.director_ids.split(',').map(id => parseInt(id, 10));
            const directorQuery = `SELECT director_name FROM directors WHERE director_id IN (${directorIds.join(',')})`;
            connection.query(directorQuery, (err, directors) => {
              if (err) {
                console.error('Error executing director query:', err.message);
                reject(err);
              }
              movie.director_names = directors.map(d => d.director_name).join(', ');
              resolve(movie);
            });
          } else {
            movie.director_names = '';
            resolve(movie);
          }
        });
      });

      Promise.all(moviePromises).then(movies => {
        res.json({
          movies: movies,
          totalItems: totalItems,
          totalPages: totalPages,
          currentPage: parseInt(page, 10)
        });
      }).catch(err => {
        console.error('Error resolving movie promises:', err);
        res.status(500).send('Internal Server Error');
      });
    });
  });
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
