require "lib/heroku-postgresql/client"
require "lib/pgbackups/client"
require "lib/heroku/commands/base"      # monkey-patch confirm-command until appears upstream
require "lib/heroku/commands/pgbackups"
require "lib/heroku/commands/pg"
