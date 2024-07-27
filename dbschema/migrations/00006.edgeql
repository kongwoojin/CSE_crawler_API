CREATE MIGRATION m122xc7r2bm5ujpquskhpuynqh6gqw3hxxqusqfrc7n7x26bdho6da
    ONTO m14or4l34gczaoktqqippvw53uzzo6zcuacvg3ekctj4mzmna7hcoa
{
  ALTER TYPE default::Files {
      ALTER PROPERTY file_url {
          RENAME TO file_uri;
      };
  };
};
