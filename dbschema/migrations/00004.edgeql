CREATE MIGRATION m1ngk4xopg5d6k4jfbybngm74yxeqqqtetmncpwn4yjggh6w2b2ujq
    ONTO m13xv4chzzd6qtwaxyzzih7nxsz3moyvtyq33zp5hgm5k3uyiaq3yq
{
  CREATE TYPE default::Files {
      CREATE REQUIRED PROPERTY file_name -> std::str;
      CREATE REQUIRED PROPERTY file_url -> std::str;
  };
  ALTER TYPE default::arch {
      DROP PROPERTY files;
  };
  ALTER TYPE default::arch {
      CREATE LINK files -> default::Files;
  };
};
