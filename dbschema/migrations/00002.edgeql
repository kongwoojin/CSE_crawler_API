CREATE MIGRATION m1utdzf5ty2koifnzgzbgfrrk4jdwdgvb3ce6ykktacc3a6muauyua
    ONTO m1dsbopymwhrltb5oc2bbyer3afwe67hnnzxr2lvvoifha4ndgztjq
{
  DROP FUTURE nonrecursive_access_policies;
  ALTER TYPE default::arch {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::cse {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::dorm {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::emc {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::ide {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::ite {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::mechanical {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::mechatronics {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::school {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
  ALTER TYPE default::sim {
      CREATE REQUIRED PROPERTY board -> std::str {
          SET REQUIRED USING ('');
      };
  };
};
