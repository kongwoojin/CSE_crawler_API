CREATE MIGRATION m1p6iwnwhnc3hhndr2liyg2dst5epfxhmphcqzmvco73yokbb6du6q
    ONTO m1fkostqrc374puheara4blfaovjpopxkk4wpgfmojudkplzwornta
{
  CREATE SCALAR TYPE default::Board EXTENDING enum<Notice, Free, Job, PDS, Lecture, Bachelor, Scholar>;
  ALTER TYPE default::notice {
      ALTER PROPERTY board {
          SET TYPE default::Board;
      };
  };
};
