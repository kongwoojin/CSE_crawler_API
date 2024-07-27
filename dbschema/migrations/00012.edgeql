CREATE MIGRATION m1buzt2gzlu5bpknmzdgxjmtqjjpmahc6osuwlfjxsat5zfqy5rh2q
    ONTO m1iayblseici3hl6522vxlllfrdmyz2eyq6eouzdi4nvv6r2t2umza
{
  CREATE SCALAR TYPE default::Department EXTENDING enum<ARCH, CSE, Dorm, EMC, IDE, ITE, Mechanical, Mechatronics, School, SIM>;
  ALTER TYPE default::notice {
      CREATE REQUIRED PROPERTY department: default::Department {
          SET REQUIRED USING (<default::Department>{});
      };
  };
};
