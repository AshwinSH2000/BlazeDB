--SELECT SUM(1), SUM(Enrolled.a) FROM Enrolled, Course, Course2, Enrolled2, Student, Student2, Teaches, teaches2;
--SELECT SUM(1), SUM(10*10) FROM Student2, Course2, Enrolled2, Enrolled;

--SELECT ENROLLED.H, Course.f FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by enrolled.h, course.f;

--SELECT Student.A, Enrolled.H FROM Course, Student, Enrolled;
--SELECT StudenT.A, EnroLLED.H FROM CouRce, STuDenT, ENROLLed;

--SELECT SUM(1), SUM(2) FROM Enrolled, Course, Course2, Enrolled2, Student, Student2, Teaches, teaches2;
--select  enrolled.a,enrolled.e from enrolled, enrolled2;
--SELECT teaches.p, Student2.A, Enrolled.H FROM Student, Student2, Course, Course2, Enrolled, Enrolled2, teaches, teaches2;
--SELECT teaches.p FROM Student, Student2, Course, Course2, Enrolled, Enrolled2, teaches, teaches2 Group by teaches.p, student.a, course.g, enrolled.h, course2.e, teaches2.m,  enrolled2.e order by teaches.p;

--SELECT Student.A, Enrolled.E from Student, Enrolled Where Student.A=2;
--SELECT teaches.p, SUM(1), SUM(Course2.E*Enrolled2.A), SUM(Student.C * 2) FROM Student, Student2, Course, Course2, Enrolled, Enrolled2, teaches, teaches2 Group by teaches.p, student.a, course.g, enrolled.h, course2.e, teaches2.m,  enrolled2.e order by teaches.p;
--SELECT SUM(1), SUM(Course2.E*Enrolled2.A), SUM(Student.C * 2) FROM Student, Student2, Course, Course2, Enrolled, Enrolled2, teaches, teaches2 Group by teaches.p, student.a, course.g, enrolled.h, course2.e;
--SELECT SUM(1), SUM(Course2.E*Enrolled2.A), SUM(Student.C * 2) FROM Student, Student2, Course, Course2, Enrolled, Enrolled2, teaches, teaches2 Group by teaches.p, student.a;
--SELECT SUM(1), SUM(Course2.E*Enrolled2.A), SUM(Student.C * 2) FROM Student, Student2, Course, Course2, Enrolled, Enrolled2, teaches;

--SELECT SUM(1), SUM(Course2.E*Enrolled2.A), SUM(Student.C * 2) FROM Student, Student2, Course, Course2, Enrolled, Enrolled2;
--SELECT SUM(1), SUM(Course.E*Course2.F), SUM(Student.C * Course.G) FROM Student, Student2, Course, Course2, Enrolled, Enrolled2;
--SELECT COURSE.F, SUM(1) FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by COURSE.F;--SELECT ENROLLED.H FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F;--SELECT Student.A, Enrolled.E, Course.F, Course.G FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Student.B > 150 AND Course.F != 5;
--SELECT Student.A, Enrolled.E, Course.F FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Student.B > 150 AND Course.F < 5;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A <= 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A >= 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A > 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A AND Student.A > 4;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE  Student.A != 4 AND Student.A = Enrolled.A;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE  Student.A > 4 AND Student.A = Enrolled.A;
--SELECT DISTINCT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A;
--SELECT * FROM Course;
--SELECT * FROM Student, Enrolled;
--SELECT * FROM Student, Enrolled WHERE Student.A > 4 AND Enrolled.H < 50;
--SELECT * FROM Student, Enrolled WHERE Student.A > 4
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A;
--SELECT ENROLLED.H FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F;
--SELECT COURSE.F, SUM(1) FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by COURSE.F;
--SELECT * FROM Student WHERE 4=Student.A;
--SELECT * FROM Student, Course WHERE Student.C < Course.E ORDER by Student.a, Course.f;
--SELECT * FROM Student, Course;
--SELECT ENROLLED.H FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by ENROLLED.H;
--SELECT COURSE.F, SUM(1) FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by COURSE.F;
--SELECT SUM(1) FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F;
--SELECT ENROLLED.H, SUM(Course.g) FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by enrolled.h;

SELECT ENROLLED.H, COURSE.F FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by Course.f, enrolled.h;
Select * from student, course, enrolled where course.e = enrolled.e and course.f = enrolled.a and student.a = enrolled.a order by student.b, enrolled.e;
--SELECT ENROLLED.H FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F;
--SELECT STUDENT.A, ENROLLED.H, COURSE.F FROM STUDENT, ENROLLED, COURSE GROUP BY STUDENT.A, ENROLLED.H, COURSE.F order by Course.f, enrolled.h, student.a;
--SELECT SUM(Student.A*Student.B), SUM(Student.C*Student.D), SUM(Student.D*3) from STUDENT;
--SELECT DISTINCT Student.A, Enrolled.E from Student, Enrolled WHERE STUDENT.A != 3 AND STUDENT.A > 3 AND STUDENT.A !=6 order by Enrolled.E;
--SELECT SUM(1), SUM(Student.A*Course.g) FROM Student, Enrolled, Course
--SELECT Enrolled.E, SUM(Enrolled.H * Enrolled.e) FROM Enrolled GROUP BY Enrolled.E;
--SELECT * FROM COURSE, ENROLLED where course.f = enrolled.a and course.e =enrolled.e;
--SELECT DISTINCT Enrolled.A from Enrolled;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A <= 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E, Course.F FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Student.B > 150 AND Course.F < 5;
--SELECT Student.A, Enrolled.E, Course.F, Course.G FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Student.B > 150 AND Course.F != 5;
--SELECT Student.A, SUM(Student.B*Student.C) FROM Student GROUP BY Student.A;
--SELECT SUM(STUDENT.A) FROM STUDENT GROUP BY STUDENT.A ORDER BY SUM(STUDENT.A);
--SELECT distinct STuDenT.d FROM COurSE, Enrolled, Student where student.d>34 group by studenT.b, studeNT.c, STuDenT.d;
--SELECT student.b,course.f FROM Student, Course GROUP BY student.b, course.f order by student.b, course.f;
--SELECT SUM(1) FROM Student GROUP BY Student.B;
--SELECT Student.D from Student Group by student.A, student.d;
--SELECT * FROM STUDENT, COURSE;

--SELECT * FROM Student, Enrolled, Course;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A;
--SELECT * FROM Student, Enrolled WHERE Student.A > 4
--SELECT * FROM Student, Enrolled WHERE Student.A > 4 AND Enrolled.H < 50;
--SELECT * FROM Student, Enrolled;
--SELECT * FROM Course;
--SELECT DISTINCT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE  Student.A > 4 AND Student.A = Enrolled.A;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE  Student.A != 4 AND Student.A = Enrolled.A;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A AND Student.A != 4;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A AND Student.A > 4;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A > 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A >= 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A <= 4 AND Student.A = Enrolled.A AND Enrolled.H < 50;
--SELECT Student.A, Enrolled.E, Course.F FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Student.B > 150 AND Course.F < 5;
--SELECT Student.A, Enrolled.E, Course.F, Course.G FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E AND Student.B > 150 AND Course.F != 5;

---

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