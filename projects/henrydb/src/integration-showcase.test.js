// integration-showcase.test.js — Feature showcase for HenryDB
// Demonstrates that advanced SQL features work together:
// CTEs, window functions, JSON, FTS, subqueries, aggregates, CASE, views
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function setupBlogDb() {
  const db = new Database();
  
  // Blog schema
  db.execute(`CREATE TABLE authors (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, bio TEXT
  )`);
  db.execute(`CREATE TABLE posts (
    id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT NOT NULL,
    body TEXT, category TEXT, published TEXT, views INTEGER DEFAULT 0
  )`);
  db.execute(`CREATE TABLE tags (
    id INTEGER PRIMARY KEY, post_id INTEGER, tag TEXT NOT NULL
  )`);
  db.execute(`CREATE TABLE comments (
    id INTEGER PRIMARY KEY, post_id INTEGER, author TEXT,
    body TEXT, rating INTEGER, created_at TEXT
  )`);
  
  // Indexes
  db.execute('CREATE INDEX idx_posts_author ON posts(author_id)');
  db.execute('CREATE INDEX idx_posts_category ON posts(category)');
  db.execute('CREATE INDEX idx_tags_post ON tags(post_id)');
  db.execute('CREATE INDEX idx_comments_post ON comments(post_id)');
  
  // Fulltext index
  db.execute('CREATE FULLTEXT INDEX idx_posts_body ON posts(body)');
  
  // Data
  const authors = [
    [1, 'Alice', 'Database engineer and writer'],
    [2, 'Bob', 'Systems programmer'],
    [3, 'Charlie', 'Full-stack developer'],
  ];
  for (const [id, name, bio] of authors) {
    db.execute(`INSERT INTO authors (id, name, bio) VALUES (${id}, '${name}', '${bio}')`);
  }
  
  const posts = [
    [1, 1, 'Understanding MVCC', 'Multi-version concurrency control allows multiple transactions to read data concurrently without blocking', 'database', '2026-01-15', 1200],
    [2, 1, 'B-Tree Internals', 'B-trees are balanced tree data structures that maintain sorted data and allow search insert and delete in logarithmic time', 'database', '2026-02-01', 3400],
    [3, 2, 'Linux Kernel Modules', 'Writing kernel modules requires understanding of the kernel API and memory management concepts', 'systems', '2026-01-20', 2100],
    [4, 2, 'TCP Congestion Control', 'Modern congestion control algorithms like BBR use bandwidth estimation rather than loss-based signals', 'networking', '2026-02-10', 980],
    [5, 3, 'React Server Components', 'Server components allow rendering React components on the server reducing client-side JavaScript bundle size', 'frontend', '2026-01-25', 4500],
    [6, 3, 'GraphQL Best Practices', 'Effective GraphQL schema design requires thinking about data relationships and query patterns upfront', 'frontend', '2026-02-15', 1800],
    [7, 1, 'WAL and Recovery', 'Write-ahead logging ensures durability by writing changes to a log before applying them to data pages', 'database', '2026-03-01', 890],
    [8, 2, 'Memory Allocators', 'Custom memory allocators can dramatically improve performance for specific allocation patterns', 'systems', '2026-03-05', 1500],
    [9, 3, 'TypeScript Generics', 'Generic types in TypeScript enable writing reusable type-safe code without sacrificing flexibility', 'frontend', '2026-03-10', 2800],
    [10, 1, 'Query Optimization', 'Cost-based query optimization uses statistics about data distribution to choose the best execution plan', 'database', '2026-03-15', 3200],
  ];
  for (const [id, aid, title, body, cat, pub, views] of posts) {
    db.execute(`INSERT INTO posts (id, author_id, title, body, category, published, views) VALUES (${id}, ${aid}, '${title}', '${body}', '${cat}', '${pub}', ${views})`);
  }
  
  const tags = [
    [1,1,'mvcc'], [2,1,'transactions'], [3,2,'btree'], [4,2,'indexing'],
    [5,3,'linux'], [6,3,'kernel'], [7,4,'networking'], [8,4,'tcp'],
    [9,5,'react'], [10,5,'ssr'], [11,6,'graphql'], [12,6,'api'],
    [13,7,'wal'], [14,7,'durability'], [15,8,'memory'], [16,8,'performance'],
    [17,9,'typescript'], [18,9,'generics'], [19,10,'optimizer'], [20,10,'statistics'],
  ];
  for (const [id, pid, tag] of tags) {
    db.execute(`INSERT INTO tags (id, post_id, tag) VALUES (${id}, ${pid}, '${tag}')`);
  }
  
  // Comments with ratings
  let cid = 1;
  const commentData = [
    [1, 'Dave', 'Excellent explanation of MVCC!', 5, '2026-01-16'],
    [1, 'Eve', 'Could use more diagrams', 3, '2026-01-17'],
    [2, 'Frank', 'Best B-tree article I have read', 5, '2026-02-02'],
    [2, 'Grace', 'Very detailed, thanks!', 4, '2026-02-03'],
    [2, 'Hank', 'Missing delete operation details', 3, '2026-02-04'],
    [3, 'Ivy', 'Helped me write my first module', 5, '2026-01-21'],
    [5, 'Jack', 'Great intro to RSC', 4, '2026-01-26'],
    [5, 'Kate', 'Changed how I think about React', 5, '2026-01-27'],
    [5, 'Leo', 'Need more real examples', 3, '2026-01-28'],
    [7, 'Mike', 'WAL is so elegant', 5, '2026-03-02'],
    [10, 'Nancy', 'Stats section is gold', 5, '2026-03-16'],
    [10, 'Oscar', 'Would love a follow-up on adaptive optimization', 4, '2026-03-17'],
  ];
  for (const [pid, author, body, rating, date] of commentData) {
    db.execute(`INSERT INTO comments (id, post_id, author, body, rating, created_at) VALUES (${cid++}, ${pid}, '${author}', '${body}', ${rating}, '${date}')`);
  }
  
  return db;
}

describe('Feature Showcase: Blog Analytics', () => {

  describe('JSON Functions', () => {
    it('JSON_EXTRACT on structured data', () => {
      const db = new Database();
      db.execute('CREATE TABLE config (id INTEGER PRIMARY KEY, data TEXT)');
      db.execute(`INSERT INTO config VALUES (1, '{"theme":"dark","lang":"en","notifications":true}')`);
      db.execute(`INSERT INTO config VALUES (2, '{"theme":"light","lang":"fr","notifications":false}')`);
      
      const r = db.execute("SELECT id, JSON_EXTRACT(data, '$.theme') as theme, JSON_EXTRACT(data, '$.lang') as lang FROM config");
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].theme, 'dark');
      assert.equal(r.rows[1].lang, 'fr');
    });

    it('JSON_TYPE identifies value types', () => {
      const db = new Database();
      db.execute('CREATE TABLE j (id INTEGER PRIMARY KEY, val TEXT)');
      db.execute(`INSERT INTO j VALUES (1, '{"a":1}')`);
      db.execute(`INSERT INTO j VALUES (2, '[1,2,3]')`);
      db.execute(`INSERT INTO j VALUES (3, '"hello"')`);
      
      const r = db.execute('SELECT id, JSON_TYPE(val) as jtype FROM j ORDER BY id');
      assert.equal(r.rows[0].jtype, 'object');
      assert.equal(r.rows[1].jtype, 'array');
      assert.equal(r.rows[2].jtype, 'string');
    });

    it('JSON_ARRAY_LENGTH on arrays', () => {
      const db = new Database();
      db.execute('CREATE TABLE j (id INTEGER PRIMARY KEY, arr TEXT)');
      db.execute(`INSERT INTO j VALUES (1, '[1,2,3,4,5]')`);
      db.execute(`INSERT INTO j VALUES (2, '[]')`);
      
      const r = db.execute('SELECT id, JSON_ARRAY_LENGTH(arr) as len FROM j ORDER BY id');
      assert.equal(r.rows[0].len, 5);
      assert.equal(r.rows[1].len, 0);
    });
  });

  describe('Full-Text Search', () => {
    it('searches blog posts by keyword', () => {
      const db = setupBlogDb();
      const r = db.execute("SELECT title FROM posts WHERE MATCH(body) AGAINST ('concurrency')");
      assert.ok(r.rows.length >= 1);
      assert.ok(r.rows.some(row => row.title.includes('MVCC')));
    });

    it('searches for technical terms', () => {
      const db = setupBlogDb();
      const r = db.execute("SELECT title FROM posts WHERE MATCH(body) AGAINST ('server')");
      assert.ok(r.rows.length >= 1);
    });

    it('no results for non-matching term', () => {
      const db = setupBlogDb();
      const r = db.execute("SELECT title FROM posts WHERE MATCH(body) AGAINST ('quantum')");
      assert.equal(r.rows.length, 0);
    });
  });

  describe('CTEs', () => {
    it('CTE: author post counts', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        WITH author_stats AS (
          SELECT author_id, COUNT(*) as post_count, SUM(views) as total_views
          FROM posts
          GROUP BY author_id
        )
        SELECT post_count, total_views
        FROM author_stats
        ORDER BY total_views DESC
      `);
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].total_views >= r.rows[1].total_views);
    });

    it('CTE: category distribution', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        WITH cat_stats AS (
          SELECT category, COUNT(*) as cnt, AVG(views) as avg_views
          FROM posts
          GROUP BY category
        )
        SELECT category, cnt, avg_views
        FROM cat_stats
        ORDER BY cnt DESC
      `);
      assert.ok(r.rows.length >= 3);
      const totalPosts = r.rows.reduce((s, r) => s + r.cnt, 0);
      assert.equal(totalPosts, 10);
    });
  });

  describe('Window Functions', () => {
    it('ROW_NUMBER: rank posts within category', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT title, category, views,
               ROW_NUMBER() OVER (PARTITION BY category ORDER BY views DESC) as rank_in_cat
        FROM posts
        WHERE category = 'database'
      `);
      assert.ok(r.rows.length >= 3);
      // Should have ranks 1 through N
      const ranks = r.rows.map(row => row.rank_in_cat).sort();
      assert.deepEqual(ranks, [1, 2, 3, 4]);
      // Rank 1 should have highest views
      const rank1 = r.rows.find(row => row.rank_in_cat === 1);
      const maxViews = Math.max(...r.rows.map(r => r.views));
      assert.equal(rank1.views, maxViews);
    });

    it('SUM OVER: running view count', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT title, views,
               SUM(views) OVER (ORDER BY published) as running_views
        FROM posts
        WHERE author_id = 1
        ORDER BY published
      `);
      assert.ok(r.rows.length >= 3);
      // Running total should be non-decreasing
      for (let i = 1; i < r.rows.length; i++) {
        assert.ok(r.rows[i].running_views >= r.rows[i-1].running_views);
      }
    });

    it('DENSE_RANK: overall popularity ranking', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT title, views,
               DENSE_RANK() OVER (ORDER BY views DESC) as popularity
        FROM posts
      `);
      assert.equal(r.rows.length, 10);
      // Rank 1 should have highest views
      const rank1 = r.rows.filter(row => row.popularity === 1);
      assert.ok(rank1.length >= 1);
      const maxViews = Math.max(...r.rows.map(r => r.views));
      assert.equal(rank1[0].views, maxViews);
    });
  });

  describe('Complex Multi-Feature Queries', () => {
    it('JOIN + GROUP BY + HAVING + ORDER BY', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT a.name, COUNT(p.id) as posts,
               SUM(p.views) as total_views,
               ROUND(AVG(p.views), 0) as avg_views
        FROM authors a
        JOIN posts p ON p.author_id = a.id
        GROUP BY a.name
        HAVING SUM(p.views) > 2000
        ORDER BY total_views DESC
      `);
      assert.ok(r.rows.length > 0);
      r.rows.forEach(row => assert.ok(row.total_views > 2000));
    });

    it('subquery + CASE: post engagement level', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT p.title, p.views,
               (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) as comment_count,
               CASE
                 WHEN p.views > 3000 THEN 'viral'
                 WHEN p.views > 1500 THEN 'popular'
                 ELSE 'normal'
               END as engagement
        FROM posts p
        ORDER BY p.views DESC
      `);
      assert.equal(r.rows.length, 10);
      assert.equal(r.rows[0].engagement, 'viral');
      // Most-viewed post should have highest views
      for (let i = 1; i < r.rows.length; i++) {
        assert.ok(r.rows[i-1].views >= r.rows[i].views);
      }
    });

    it('multi-table join: posts with tags and comment stats', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT p.title, a.name as author,
               COUNT(DISTINCT t.id) as tag_count,
               COUNT(DISTINCT c.id) as comment_count
        FROM posts p
        JOIN authors a ON p.author_id = a.id
        LEFT JOIN tags t ON t.post_id = p.id
        LEFT JOIN comments c ON c.post_id = p.id
        GROUP BY p.title, a.name
        ORDER BY comment_count DESC
      `);
      assert.equal(r.rows.length, 10);
      // Every post should have at least 1 tag (we added 2 per post)
      r.rows.forEach(row => assert.ok(row.tag_count >= 1));
    });

    it('UNION: combined author activity', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT 'post' as type, title as detail, views as metric
        FROM posts WHERE author_id = 1
        UNION
        SELECT 'comment' as type, body as detail, rating as metric
        FROM comments WHERE author = 'Dave'
      `);
      assert.ok(r.rows.length > 0);
    });

    it('EXISTS + nested subquery: authors with highly rated posts', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT a.name FROM authors a
        WHERE EXISTS (
          SELECT 1 FROM posts p
          WHERE p.author_id = a.id
          AND EXISTS (
            SELECT 1 FROM comments c
            WHERE c.post_id = p.id AND c.rating = 5
          )
        )
      `);
      assert.ok(r.rows.length > 0);
    });

    it('IN subquery: categories with popular posts', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT DISTINCT category FROM posts
        WHERE id IN (
          SELECT post_id FROM comments WHERE rating >= 4
        )
        ORDER BY category
      `);
      assert.ok(r.rows.length > 0);
    });
  });

  describe('Views', () => {
    it('creates and queries author leaderboard view', () => {
      const db = setupBlogDb();
      db.execute(`
        CREATE VIEW leaderboard AS
        SELECT a.name, COUNT(p.id) as posts,
               SUM(p.views) as total_views
        FROM authors a
        JOIN posts p ON p.author_id = a.id
        GROUP BY a.name
      `);
      const r = db.execute('SELECT * FROM leaderboard ORDER BY total_views DESC');
      assert.equal(r.rows.length, 3);
    });

    it('view with filter: top posts per category', () => {
      const db = setupBlogDb();
      db.execute(`
        CREATE VIEW category_top AS
        SELECT category, title, views
        FROM posts
        WHERE views > 1000
      `);
      const r = db.execute('SELECT * FROM category_top ORDER BY views DESC');
      assert.ok(r.rows.length > 0);
      r.rows.forEach(row => assert.ok(row.views > 1000));
    });
  });

  describe('CREATE TABLE AS', () => {
    it('materializes engagement report', () => {
      const db = setupBlogDb();
      db.execute(`
        CREATE TABLE engagement_report AS
        SELECT p.title, p.category, p.views,
               COUNT(c.id) as comments,
               AVG(c.rating) as avg_rating
        FROM posts p
        LEFT JOIN comments c ON c.post_id = p.id
        GROUP BY p.title, p.category, p.views
      `);
      const r = db.execute('SELECT * FROM engagement_report ORDER BY views DESC');
      assert.equal(r.rows.length, 10);
    });
  });

  describe('Analytics Patterns', () => {
    it('BETWEEN: date range filter', () => {
      const db = setupBlogDb();
      const r = db.execute("SELECT title FROM posts WHERE published BETWEEN '2026-02-01' AND '2026-02-28' ORDER BY published");
      assert.ok(r.rows.length > 0);
    });

    it('LIKE: title pattern search', () => {
      const db = setupBlogDb();
      const r = db.execute("SELECT title FROM posts WHERE title LIKE '%Query%'");
      assert.ok(r.rows.length >= 1);
    });

    it('COALESCE with LEFT JOIN: all authors including those without comments', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT a.name, COALESCE(SUM(p.views), 0) as total_views
        FROM authors a
        LEFT JOIN posts p ON p.author_id = a.id
        GROUP BY a.name
        ORDER BY total_views DESC
      `);
      assert.equal(r.rows.length, 3);
    });

    it('LIMIT + OFFSET: paginated results', () => {
      const db = setupBlogDb();
      const p1 = db.execute('SELECT title FROM posts ORDER BY views DESC LIMIT 3');
      const p2 = db.execute('SELECT title FROM posts ORDER BY views DESC LIMIT 3 OFFSET 3');
      assert.equal(p1.rows.length, 3);
      assert.equal(p2.rows.length, 3);
      // Pages shouldn't overlap
      const titles1 = new Set(p1.rows.map(r => r.title));
      p2.rows.forEach(row => assert.ok(!titles1.has(row.title)));
    });

    it('COUNT DISTINCT: unique commenters per post', () => {
      const db = setupBlogDb();
      const r = db.execute(`
        SELECT p.title, COUNT(DISTINCT c.author) as unique_commenters
        FROM posts p
        LEFT JOIN comments c ON c.post_id = p.id
        GROUP BY p.title
        ORDER BY unique_commenters DESC
      `);
      assert.equal(r.rows.length, 10);
    });
  });
});
