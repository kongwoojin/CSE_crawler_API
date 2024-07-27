CREATE MIGRATION m17a22nzvio7om4d5fwjccyfpienbi2vjfqhy7a2pe63rhvvxvn6ra
    ONTO m1ta35icmzuz47wqqkcpnb3ddsjaugrt6rmpmpssb46e4ugem3r55a
{
  ALTER TYPE default::Files {
      ALTER PROPERTY file_uri {
          RENAME TO file_url;
      };
  };
};
