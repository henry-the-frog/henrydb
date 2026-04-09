// tutorial.js — Interactive SQL tutorial for HenryDB playground
// Embeddable lesson system with progressive challenges

export const tutorials = [
  {
    id: 'basics',
    title: '🎓 SQL Basics',
    lessons: [
      {
        title: 'Your First Query',
        description: 'Use SELECT to retrieve data from a table.',
        hint: 'SELECT columns FROM table_name',
        setup: `CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
INSERT INTO students VALUES (1, 'Alice', 22);
INSERT INTO students VALUES (2, 'Bob', 25);
INSERT INTO students VALUES (3, 'Charlie', 23);`,
        challenge: 'Select all columns from the students table.',
        solution: 'SELECT * FROM students;',
        check: (rows) => rows && rows.length === 3 && rows[0].name !== undefined,
      },
      {
        title: 'Filtering with WHERE',
        description: 'Use WHERE to filter rows based on conditions.',
        hint: 'SELECT * FROM table WHERE condition',
        setup: null, // reuse previous
        challenge: 'Find students older than 22.',
        solution: "SELECT * FROM students WHERE age > 22;",
        check: (rows) => rows && rows.length === 2,
      },
      {
        title: 'Ordering Results',
        description: 'Use ORDER BY to sort results. ASC = ascending, DESC = descending.',
        hint: 'SELECT * FROM table ORDER BY column DESC',
        setup: null,
        challenge: 'List all students sorted by age, oldest first.',
        solution: 'SELECT * FROM students ORDER BY age DESC;',
        check: (rows) => rows && rows.length === 3 && rows[0].name === 'Bob',
      },
      {
        title: 'Selecting Specific Columns',
        description: 'List only the columns you need instead of using *.',
        hint: 'SELECT col1, col2 FROM table',
        setup: null,
        challenge: 'Show only the name and age of each student.',
        solution: 'SELECT name, age FROM students;',
        check: (rows) => rows && rows.length === 3 && rows[0].id === undefined && rows[0].name !== undefined,
      },
    ],
  },
  {
    id: 'aggregates',
    title: '📊 Aggregates & Grouping',
    lessons: [
      {
        title: 'COUNT, SUM, AVG',
        description: 'Aggregate functions compute values across multiple rows.',
        hint: 'SELECT COUNT(*) as total FROM table',
        setup: `CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL, region TEXT);
INSERT INTO sales VALUES (1, 'Widget', 29.99, 'North');
INSERT INTO sales VALUES (2, 'Gadget', 49.99, 'South');
INSERT INTO sales VALUES (3, 'Widget', 29.99, 'South');
INSERT INTO sales VALUES (4, 'Gizmo', 99.99, 'North');
INSERT INTO sales VALUES (5, 'Widget', 29.99, 'North');
INSERT INTO sales VALUES (6, 'Gadget', 49.99, 'North');`,
        challenge: 'Find the total number of sales and the total amount.',
        solution: 'SELECT COUNT(*) as total_sales, SUM(amount) as total_amount FROM sales;',
        check: (rows) => rows && rows[0].total_sales === 6,
      },
      {
        title: 'GROUP BY',
        description: 'Group rows that have the same value and aggregate per group.',
        hint: 'SELECT column, COUNT(*) FROM table GROUP BY column',
        setup: null,
        challenge: 'Count sales per product.',
        solution: 'SELECT product, COUNT(*) as cnt FROM sales GROUP BY product;',
        check: (rows) => rows && rows.length === 3,
      },
      {
        title: 'HAVING',
        description: 'Filter groups after aggregation (WHERE filters before).',
        hint: 'SELECT ... GROUP BY ... HAVING condition',
        setup: null,
        challenge: 'Find products with more than 1 sale.',
        solution: 'SELECT product, COUNT(*) as cnt FROM sales GROUP BY product HAVING COUNT(*) > 1;',
        check: (rows) => rows && rows.length === 2,
      },
    ],
  },
  {
    id: 'joins',
    title: '🔗 JOINs',
    lessons: [
      {
        title: 'INNER JOIN',
        description: 'Combine rows from two tables where a condition matches.',
        hint: 'SELECT ... FROM a JOIN b ON a.col = b.col',
        setup: `CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO departments VALUES (1, 'Engineering');
INSERT INTO departments VALUES (2, 'Marketing');
INSERT INTO departments VALUES (3, 'Finance');
CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL);
INSERT INTO employees VALUES (1, 'Alice', 1, 95000);
INSERT INTO employees VALUES (2, 'Bob', 1, 87000);
INSERT INTO employees VALUES (3, 'Charlie', 2, 72000);
INSERT INTO employees VALUES (4, 'Diana', 3, 68000);
INSERT INTO employees VALUES (5, 'Eve', 1, 91000);`,
        challenge: 'Show each employee with their department name.',
        solution: `SELECT e.name, d.name as department
FROM employees e
JOIN departments d ON e.dept_id = d.id;`,
        check: (rows) => rows && rows.length === 5 && rows[0].department !== undefined,
      },
      {
        title: 'JOIN with Aggregates',
        description: 'Combine JOINs with GROUP BY for cross-table analytics.',
        hint: 'SELECT ... FROM a JOIN b ON ... GROUP BY ...',
        setup: null,
        challenge: 'Find average salary per department.',
        solution: `SELECT d.name as department, AVG(e.salary) as avg_salary
FROM employees e
JOIN departments d ON e.dept_id = d.id
GROUP BY d.name;`,
        check: (rows) => rows && rows.length === 3,
      },
    ],
  },
  {
    id: 'advanced',
    title: '🚀 Advanced Queries',
    lessons: [
      {
        title: 'Subqueries',
        description: 'Use a query inside another query.',
        hint: 'SELECT ... WHERE col IN (SELECT ...)',
        setup: `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT);
INSERT INTO products VALUES (1, 'Laptop', 999, 'Electronics');
INSERT INTO products VALUES (2, 'Phone', 699, 'Electronics');
INSERT INTO products VALUES (3, 'Desk', 299, 'Furniture');
INSERT INTO products VALUES (4, 'Chair', 199, 'Furniture');
INSERT INTO products VALUES (5, 'Tablet', 499, 'Electronics');`,
        challenge: 'Find products that cost more than the average price.',
        solution: 'SELECT name, price FROM products WHERE price > (SELECT AVG(price) FROM products);',
        check: (rows) => rows && rows.length === 2,
      },
      {
        title: 'CASE Expressions',
        description: 'Add conditional logic to your queries.',
        hint: 'CASE WHEN condition THEN value ELSE default END',
        setup: null,
        challenge: 'Classify each product as "Expensive" (>500), "Mid" (200-500), or "Cheap" (<200).',
        solution: `SELECT name, price,
  CASE WHEN price > 500 THEN 'Expensive'
       WHEN price >= 200 THEN 'Mid'
       ELSE 'Cheap' END as tier
FROM products ORDER BY price DESC;`,
        check: (rows) => rows && rows.length === 5 && rows[0].tier === 'Expensive',
      },
      {
        title: 'Window Functions',
        description: 'Compute values across related rows without collapsing them.',
        hint: 'ROW_NUMBER() OVER (ORDER BY column)',
        setup: null,
        challenge: 'Rank products by price (most expensive = rank 1).',
        solution: `SELECT name, price,
  ROW_NUMBER() OVER (ORDER BY price DESC) as rank
FROM products;`,
        check: (rows) => rows && rows.length === 5 && rows.some(r => r.rank === 1),
      },
    ],
  },
];

export function getTutorialList() {
  return tutorials.map(t => ({
    id: t.id,
    title: t.title,
    lessonCount: t.lessons.length,
  }));
}

export function getLesson(tutorialId, lessonIndex) {
  const tutorial = tutorials.find(t => t.id === tutorialId);
  if (!tutorial) return null;
  const lesson = tutorial.lessons[lessonIndex];
  if (!lesson) return null;
  return { ...lesson, tutorialTitle: tutorial.title, total: tutorial.lessons.length, index: lessonIndex };
}
