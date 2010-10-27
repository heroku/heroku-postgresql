module PgUtils
  def spinner(ticks)
    %w(/ - \\ |)[ticks % 4]
  end

  def redisplay(line, line_break = false)
    display("\r\e[0K#{line}", line_break)
  end

  def display_info(label, info)
    display(format("%-12s %s", label, info))
  end

  def pg_config_var_names
    # all config vars that are a postgres:// URL
    pg_config_vars = @config_vars.reject { |k,v| not v =~ /^postgres:\/\// }
    pg_config_vars.keys.sort!
  end

  def resolve_db_id(input, opts={})
    name = input || opts[:default]

    # try to find addon config var name from all config vars
    # if name is 'DATABASE_URL', try to return the addon config var name for better accounting
    output = nil
    addon_config_vars = pg_config_var_names - ["DATABASE_URL"]
    addon_config_vars.each { |n|
      next unless @config_vars[n] == @config_vars[name]
      output = [n, @config_vars[n]]
    }

    # database url isn't an alias for another var
    output = [name, @config_vars[name]] if !output && name == "DATABASE_URL"

    if !input && opts[:default]
      display "=== No database specified via --db, selecting a default."

      var_names = pg_config_var_names
      var_names = var_names - ["DATABASE_URL"] unless var_names.any? { |v| @config_vars[v] == @config_vars["DATABASE_URL"] }

      result = (pg_config_var_names - ["DATABASE_URL"]).map do |var|
        str = var
        if @config_vars[var] == @config_vars["DATABASE_URL"]
          str += " (DATABASE_URL)"
        end

        if var == output[0]
          "[#{str}]"
        else
          "#{str}"
        end
      end.join(", ")
      display "=== #{result}"
    end

    return *output if output

    abort("Database #{name} not found in config. (Options are: #{pg_config_var_names.join(', ')})") if name
    abort(" !   Rerun this command with a database to promote: \n !   #{pg_config_var_names.join(', ')}") unless opts[:default]

  end

end
