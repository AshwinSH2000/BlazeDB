Select * from student, course, enrolled where course.e = enrolled.e and course.f = enrolled.a and student.a = enrolled.a order by student.b, enrolled.e;

--SELECT * FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E;

--SELECT * FROM Student, Course, Enrolled WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E;

--SELECT * FROM Student, Course WHERE Student.A=2;

--SELECT DISTINCT SUM(1) FROM STUDENT, ENROLLED;

--SELECT SUM(1), SUM(Student.A*Course.E), Enrolled.E from Student, Course, Enrolled WHERE Course.G=Student.A Group by Student.A, Course.E , Enrolled.E ORDER By Enrolled.E, SUM(Student.A*Course.E);

--SELECT * FROM Student, Enrolled, Course, Teaches, Professor WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Course.E = Teaches.E AND teaches.M = Professor.M;

--SELECT * FROM Student, Course, Enrolled WHERE Enrolled.A = Student.A AND Enrolled.h<50 order by student.a;

--this type of query does cross product to maintain the order of tuples. and then filters the rows based on the where condition. 
--satisfies the condition but efficiency gone

--SELECT * FROM Student, Course, Enrolled WHERE Enrolled.E = Course.E order by Student.a;



--SELECT Student.A, Course.F, SUM(Enrolled.H) FROM Student, Course, Enrolled WHERE Student.A = Enrolled.A AND Course.E = Enrolled.E GROUP BY Student.A, Course.F;

--SELECT Student.A, Student.B, Student.C FROM Student ORDER BY Student.A, Student.B, Student.C;

--SELECT * FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A;

--SELECT Student.B, SUM(Student.A * Student.B) FROM Student GROUP BY Student.B order by SUM(Student.A * Student.B);

--SELECT DISTINCT Student.a, Course.f FROM Student JOIN Enrolled ON Student.a = Enrolled.a JOIN Course ON Enrolled.e = Course.e;