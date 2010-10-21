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
end