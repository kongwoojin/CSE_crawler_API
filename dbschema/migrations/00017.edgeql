CREATE MIGRATION m1bmlqvzb5nv6uaxunhpy4lmrzwfx6wm5x2zpi3acag2iyy7zbdjpq
    ONTO m1uukfmgsbazav2bwjokowcpilrpqdpzbi5dxelpnhi3kb5lgptbva
{
  ALTER SCALAR TYPE default::Department EXTENDING enum<ARCH, CSE, DORM, MSE, ACE, IDE, ITE, MECHANICAL, MECHATRONICS, SCHOOL, SIM>;
};
