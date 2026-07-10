CREATE TABLE Departments (
    DepartmentID INTEGER PRIMARY KEY,
    DepartmentName VARCHAR(50),
    Location VARCHAR(50)
);

INSERT INTO Departments
(DepartmentID, DepartmentName, Location)
VALUES
(1,'HR','London'),
(2,'Finance','Frankfurt'),
(3,'IT','Berlin'),
(4,'Sales','Paris'),
(5,'Marketing','Madrid');

CREATE TABLE Employees (
 EmployeeID INTEGER PRIMARY KEY,
 FirstName VARCHAR(50),
 LastName VARCHAR(50),
 Gender VARCHAR(10),
 Age INTEGER,
 DepartmentID INTEGER,
 City VARCHAR(50),
 Salary INTEGER,
 FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

INSERT INTO Employees
(EmployeeID, FirstName, LastName, Gender, Age, DepartmentID, City, Salary)
VALUES
(101,'Emma','Wilson','Female',28,1,'London',45000),
(102,'Liam','Smith','Male',35,2,'Manchester',62000),
(103,'Sophia','Brown','Female',31,3,'Berlin',70000),
(104,'Noah','Taylor','Male',42,4,'Paris',68000),
(105,'Olivia','Martin','Female',26,5,'Madrid',48000),
(106,'Lucas','Muller','Male',38,3,'Munich',82000),
(107,'Isabella','Garcia','Female',30,2,'Barcelona',61000),
(108,'Ethan','Johnson','Male',45,1,'Dublin',75000),
(109,'Mia','Anderson','Female',27,4,'Amsterdam',52000),
(110,'James','Thomas','Male',33,3,'London',73000);


--1.Display all Employees--

SELECT * FROM Employees;

--2. Display only the employee names and salaries--Departments
SELECT FirstName || ' ' || LastName AS FullName,Salary FROM Employees;

--3. Count the total number of Employees--

SELECT EmployeeID ,COUNT(*) AS total_employees FROM Employees
GROUP BY EmployeeID
ORDER BY EmployeeID;

--4.Display all unique cities-- 
SELECT DISTINCT City FROM Employees;

--5.. Display all unique department IDs.-- 

SELECT DISTINCT DepartmentID FROM Departments;

--6. Find employees older than 30.--

SELECT EmployeeID,FirstName,LastName,Age
FROM Employees
WHERE Age > 30
GROUP BY EmployeeID;

--7. Find employees earning more than 60,000.--

SELECT EmployeeID,FirstName,LastName,salary
FROM Employees
GROUP BY EmployeeID
HAVING Salary > 60000;

--8.Display employees from London--

SELECT EmployeeID,City
FROM Employees
WHERE City IS 'London'
GROUP BY EmployeeID;

--9.Find employees whose salary is between 50,000 and 75,000--

SELECT EmployeeID,salary
FROM Employees
GROUP BY EmployeeID 
HAVING Salary BETWEEN 50000 and 75000;

--10. Display employees whose first name starts with L--

SELECT EmployeeID,Firstname
FROM Employees
WHERE FirstName LIKE 'L%';

--11.Display employees whose age is less than 35--

SELECT EmployeeID,Age 
FROM Employees
WHERE Age < 35;

--12.Sort employees by salary (highest first).--

SELECT EmployeeID,Salary 
FROM Employees
ORDER BY Salary DESC;

--13.Sort employees by age (youngest first)..-- 

SELECT EmployeeID,Age 
FROM Employees
ORDER BY Age;

--14.Sort employees by city and then salary.--

SELECT EmployeeID,City,Salary
FROM Employees
ORDER BY City, Salary;

--15.Find the average salary -- 

SELECT AVG(Salary) AS avg_salary
FROM Employees;

--16. Find the Highest Salary-- 

SELECT MAX(Salary) AS Highest_Salary
FROM Employees;

--17. Find the minimum Salary-- 

SELECT MIN(Salary) AS minimum_salary
FROM Employees;

--18. Find the Average Employees age-- 

SELECT AVG(Age) AS average_emp_age
FROM Employees;

--19.Count employees in each department-- 

SELECT DepartmentID, COUNT(*) AS employee_per_department
FROM Employees
GROUP BY departmentID;

--20. Find the average salary in each department--

SELECT DepartmentID, AVG(Salary) AS avg_salary_per_department
FROM Employees
GROUP BY DepartmentID;

--21.Find the highest salary in each department.-- 

SELECT DepartmentID, MAX(Salary) as Highest_salary_per_department
FROM Employees
GROUP BY DepartmentID;

--22. Show only departments having more than one employee-- 

SELECT DepartmentID,COUNT(EmployeeID) AS emp_count
FROM Employees
GROUP BY DepartmentID
HAVING emp_count > 1;

-- 23.Increase salaries of IT employees by 5%.--

UPDATE Employees
SET Salary = Salary * 1.05;

--24.Change the city of EmployeeID 109 to Brussels.--

UPDATE Employees
SET City = 'Brussels'
WHERE EmployeeID = 109;

--25.Delete employees whose salary is below 48,000.-- 

DELETE FROM Employees
WHERE Salary < 48000;

--26.Display each employee along with their department name.--

SELECT e.EmployeeID, e.FirstName, d.DepartmentName
FROM Employees e 
JOIN Departments d ON e.DepartmentID=d.DepartmentID;


--27. Display employee name, department name, and department location.

SELECT e.FirstNAme,e.LastName,d.DepartmentName,d.Location
FROM Employees e 
LEFT JOIN Departments d ON e.DepartmentID=d.DepartmentID;

--.28. Count the number of employees in each department using a JOIN-- 

SELECT COUNT(e.EmployeeID) AS emp_count,d.DepartmentID
FROM Employees e 
INNER JOIN Departments d ON e.DepartmentID=d.DepartmentID 
GROUP BY d.DepartmentID
ORDER BY d.DepartmentID;

--29.Display all departments, even if they have no employees-- 

SELECT d.DepartmentID,e.EmployeeID,d.DepartmentName
FROM Departments d 
LEFT JOIN Employees e ON d.DepartmentID=e.DepartmentID
ORDER BY d.DepartmentID;

--30: Find the average salary for each department using a JOIN--

SELECT d.DepartmentID,d.DepartmentName,AVG(e.Salary) AS avg_salary_per_department
FROM Departments d 
LEFT JOIN Employees e ON d.DepartmentID=e.DepartmentID 
GROUP BY d.DepartmentID
ORDER BY d.DepartmentID;

--31.Display employees who work in departments located in Berlin-- 

SELECT d.Location,e.EmployeeID,e.FirstName
FROM Employees e 
JOIN Departments d ON e.DepartmentID=d.DepartmentID
WHERE d.Location = 'Berlin';

--32. Display only employees working in the IT department --

SELECT e.EmployeeID,e.FirstName,d.DepartmentName
FROM Employees e 
JOIN Departments d ON e.DepartmentID=d.DepartmentID
WHERE d.DepartmentName = 'IT';

--33.Display the Top 3 highest-paid employees-- 

SELECT EmployeeID,FirstName,LastName,Salary
FROM Employees  
ORDER BY Salary DESC LIMIT 3;





