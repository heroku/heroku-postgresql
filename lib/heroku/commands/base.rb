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
    