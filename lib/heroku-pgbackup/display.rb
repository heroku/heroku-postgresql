class Display
  attr_reader :columns, :rows

  def initialize(columns=nil, rows=nil)
    @columns = columns
    @rows = rows
  end

  def render(data)
    _data = data
    data = DataSource.new(data, :display_columns => @columns)

    # join in grid lines
    lines = []
    data.rows.each { |row|
      lines << row.join(" | ")
    }

    # insert header grid line
    if _data.length > 1
      grid_row = data.rows.first.map { |datum| "-" * datum.length }
      grid_line = grid_row.join("-+-")
      lines.insert(1, grid_line)
      lines << "" # trailing newline
    end
    return lines.join("\n")
  end

  class DataSource
    attr_reader :rows, :columns

    def initialize(data, opts={})
      rows = data.flatten(1)
      columns = rows.transpose

      max_widths = columns.map { |c|
        c.map { |datum| datum.length }.max
      }

      max_widths = [10, 10] if opts[:display_columns]

      @columns = []
      columns.each_with_index { |c,i|
        column = @columns[i] = []
        c.each { |d| column << d.ljust(max_widths[i]) }
      }
      @rows = @columns.transpose
    end
  end
end