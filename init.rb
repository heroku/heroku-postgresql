require "heroku-postgresql/client"
require "pgbackups/client"
require "heroku/commands/pgutils"
require "heroku/commands/pgbackups"
require "heroku/commands/pg"

# monkey-patch in confirm_command until accepted upstread
module Heroku::Command
  class Base
    def confirm_command(message="This command requires confirmation.")
      if extract_option('--force')
        display "Command execution forced. Proceeding."
        return true
      end

      confirmed_app = extract_option('--confirm', false)
      display message
      unless confirmed_app == app
        raise(CommandFailed, "Add '--confirm #{app}' to execute this command.")
      else
        display("Command confirmed for #{app}. Proceeding.")
      end

      true
    end
  end
end
