SELECT * FROM Student, Enrolled, Course  WHERE Course.E = Enrolled.E AND Student.A = Enrolled.A;
