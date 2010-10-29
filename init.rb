require "heroku-postgresql/client"
require "pgbackups/client"
require "heroku/commands/pgutils"
require "heroku/commands/pgbackups"
require "heroku/commands/pg"

# monkey-patch in confirm_command until accepted upstread
module Heroku::Command
  class Base
    def confirm_command
      confirmed_app = extract_option('--confirm', false)

      if confirmed_app != app
        display "\n !    Potentially Destructive Action"
        display " !    To proceed, re-run this command with --confirm #{@app}"

        false
      else
        true
      end
    end
  end
end
