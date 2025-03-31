SELECT DISTINCT Student.a, Course.f FROM Student, Enrolled, Course WHERE Student.a = Enrolled.a AND Enrolled.e = Course.e;
