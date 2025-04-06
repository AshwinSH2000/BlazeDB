select sum(student.a * student.b), enrolled.h from Student, enrolled where student.a=enrolled.a group by student.a, enrolled.h;
--SELECT Student.A, Enrolled.E FROM Student, Enrolled WHERE Student.A = Enrolled.A AND Student.A != 4;
