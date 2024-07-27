CREATE MIGRATION m1fkostqrc374puheara4blfaovjpopxkk4wpgfmojudkplzwornta
    ONTO m1i5hpkcz2hsktawhhznng2kf4t6xw2pzaaa7qidff2g7jra7ruyda
{
  ALTER SCALAR TYPE default::Department EXTENDING enum<ARCH, CSE, DORM, EMC, IDE, ITE, MECHANICAL, MECHATRONICS, SCHOOL, SIM>;
};
