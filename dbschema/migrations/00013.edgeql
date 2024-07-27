CREATE MIGRATION m1i5hpkcz2hsktawhhznng2kf4t6xw2pzaaa7qidff2g7jra7ruyda
    ONTO m1buzt2gzlu5bpknmzdgxjmtqjjpmahc6osuwlfjxsat5zfqy5rh2q
{
  CREATE SCALAR TYPE default::Category EXTENDING enum<None, Notice, EA, CA, Work, ETC>;
  ALTER TYPE default::notice {
      CREATE REQUIRED PROPERTY category: default::Category {
          SET REQUIRED USING (<default::Category>{default::Category.None});
      };
  };
};
