SELECT * FROM Student, Course, Enrolled WHERE Enrolled.E = Course.E order by Student.a;

--SELECT * FROM Student, Enrolled, Course  WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E;

--SELECT * FROM Student, Course, Enrolled WHERE Student.A = Enrolled.A AND Enrolled.E = Course.E;

--SELECT Student.A, Course.F, SUM(Enrolled.H) FROM Student, Course, Enrolled WHERE Student.A = Enrolled.A AND Course.E = Enrolled.E GROUP BY Student.A, Course.F;

--SELECT Student.A, Student.B, Student.C FROM Student ORDER BY Student.A, Student.B, Student.C;

--SELECT * FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A;

--SELECT Student.B, SUM(Student.A * Student.B) FROM Student GROUP BY Student.B order by SUM(Student.A * Student.B);

--SELECT DISTINCT Student.a, Course.f FROM Student JOIN Enrolled ON Student.a = Enrolled.a JOIN Course ON Enrolled.e = Course.e;