require "pg"
require "colorize"
require "../config/orm_config"
require "./currency"
require "./orm_joins"
require "./orm_transactional"

module MacroOrmConnection
  class_property db : DB::Database? = nil
end

class MacroOrm
  include MacroOrmJoins
  include MacroOrmTransactional
  CUSTOM_TYPES = [] of Nil
  
  # class_property connection : DB::Database? = nil
  class_property conn_mutex = Mutex.new

  def self.connected?
    ::MacroOrmConnection.db != nil
  end

  def self.disconnect
    ::MacroOrmConnection.db = nil
  end

  def self.db : DB::Database
    print "."
    ::MacroOrm.conn_mutex.lock
    unless connected?
      puts "MacroOrm: open new db connection"
      if tx = ::MacroOrmTransactional.tx
        tx.close 
      end
      ::MacroOrmConnection.db = DB.open(CONNECTION_URL)
    else
      if ::MacroOrmTransactional.tx
        puts "Request Orm.db while transaction active!".colorize(:yellow) 
      end
    end
    ::MacroOrmConnection.db || raise "#{MacroOrm} could not open #{CONNECTION_URL}"
  ensure
    ::MacroOrm.conn_mutex.unlock
  end

  macro get_connection
    {% if flag?(:txdebug) %}
      if ::MacroOrmTransactional.tx
        print "tx ".colorize :yellow
      else
        print "db ".colorize :yellow
      end
    {% end %}\
    (
      if %tx = ::MacroOrmTransactional.tx
        %tx.connection
      else
        MacroOrm.db
      end
    )
  end

  macro get_type(id)
    {{id.camelcase}}
  end

  macro map(mapping, strict = true)
    {%
      debug = flag?(:timings)
      type_names = @type.name.split("::")
      type_name = type_names.last.id
      table_const = @type.constant("Table")
      table_name = (table_const && table_const.id) || type_name.underscore.id
    %}\
    {% unless table_const %}\
      Table = {{table_name.stringify}}
    {% end %}\

    {% for key, value in mapping %}\
      {% mapping[key] = {type: value} unless value.is_a?(HashLiteral) || value.is_a?(NamedTupleLiteral) %}\
    {% end %}\

    {% primaryName = nil 
      # TODO: rename to primary, think about muliply primaries 
    %}\
    {% for key, value in mapping %}\
      {% if value[:type].is_a?(Generic) %}\
        {% uni = value[:type].type_vars.map(&.resolve) %}\
        {% value[:nilable] = true if uni.includes?(Nil) %}\
        {% value[:type] = uni[0] if uni.size==2 %}\
      {% end %}\
      {% if key.id == :id.id %}\
        {% primaryName = key.id 
         primaryType = value[:type] %}\
      {% end %}\ 

      {% if value[:type].is_a?(Call) && value[:type].name == "|" &&
              (value[:type].receiver.resolve == Nil || value[:type].args.map(&.resolve).any?(&.==(Nil))) %}\
        {% value[:nilable] = true %}\
      {% elsif value[:type].is_a?(Generic) && value[:type].name.id == "Union".id %}\
        {% puts "Unresolved #{key} : #{ value[:type].type_vars } and nilable = #{ value[:nilable] }" %}\
      {% end %}\
      {% value[:type] = value[:type].resolve if value[:type].is_a?(Path) %}\
    {% end %}\

    {% for key, value in mapping %}\
      {% type_def = value[:type] %}\
      
      {% if type_def.has_attribute? Flags.id %}\
        {% raise "Custom type #{type_def} must have 'value' method" unless valueMethod = type_def.methods.find{|x| x.name == "value".id }
        kind = type_def.constant("None").kind
        value[:internal_type] = if kind == :i16
            Int16
          elsif kind == :i32
            Int32
          elsif kind == :i64
            Int64
          else raise "unknown flags kind"
          end
        value[:key] = "#{CUSTOM_TYPE_PREFIX.id}#{key}#{CUSTOM_TYPE_POSTFIX.id}" %}\
        macro {{key}}_all?(*flags)
          "{{table_name}}.{{value[:key].id}}&#{ {{value[:type]}}.flags(\{{flags.splat}}).value }=#{ {{value[:type]}}.flags(\{{flags.splat}}).value }"
        end

        macro {{key}}_not_set?(flag)
          "{{table_name}}.{{value[:key].id}}&#{ {{value[:type]}}::\{\{flag}}.value }=0"
        end
      {% elsif !DB::Any.union_types.includes?(type_def) && type_def.id != Int16.id %}\
        {% unless CUSTOM_TYPES.any?{|x| x.id == type_def.id }
          puts "New custom type detected: #{ type_def } (#{ type_def.class_name.id })".id
          CUSTOM_TYPES << type_def
        end %}\
        {% valueMethod = type_def.methods.find{|x| x.name == "value".id }
        raise "Custom type #{type_def} must have .value method with explicit return type" unless valueMethod && valueMethod.return_type
        value[:internal_type] = valueMethod.return_type
        value[:key] = "_#{key}"%}\
      {% end %}\
    {% end %}\

    Fields = { {{  mapping.map{  |k,v| k.underscore.symbolize  }.splat  }} }
    Query = { {{  mapping.map{  |k,v| v[:query] || (v[:key] || k).id.symbolize  }.splat  }} }
    # StorageInternals = { {{  mapping.map{  |k,v| "#{ v[:internal_type] || v[:type] }#{ "|Nil".id if v[:nilable] }".id  }.splat  }} }

    {% for key, value in mapping %}\
      @{{key.id}} : {{value[:type]}} {{ (value[:nilable] ? "?" : "").id }}
      def {{key.id}}=(_{{key.id}} : {{value[:type]}} {{ (value[:nilable] ? "?" : "").id }})
        @{{key.id}} = _{{key.id}}
      end
      def {{key.id}}
        @{{ (primaryName == key.id ? "#{key} || raise(\"touching nil #{@type.name}.#{primaryName}\")" : key).id }}
      end
    {% end %}\

    {% if primaryName %}\
    def exists?
      {% if mapping[primaryName][:nilable] %}\
        @id && @id!=0 ? true : nil
      {% else %}\
        true
      {% end %}\
    end
    {% end %}\

    def self.from_rs(%rs : ::DB::ResultSet)
      %objs = Array({{ type_name }}).new
      %rs.each do
        %objs << {{ type_name }}.new(%rs)
      end
      %objs
    ensure
      %rs.close
    end

    def initialize(%rs : ::DB::ResultSet) : Nil
      {% index = 0 %}
      {% for key, value in mapping %}\
        value = {% if value[:converter] %}\
          {% raise "do not use converter with custom type #{value[:type]}." if value[:internal_type] %}\
          {% raise "Using converter will not work with tables joins, TODO: rewrite" %}\
          {{value[:converter]}}.from_rs(%rs)
        {% elsif db_type = value[:internal_type] && !value[:type].has_attribute?(Flags.id) %}\
          {{value[:type]}}.from_rs(%rs)
        {% elsif db_type = value[:internal_type] %}\
          {{value[:type]}}.new(
          {% if value[:nilable] || value[:default] != nil %}\
            %rs.read(::Union({{db_type}} | Nil))
          {% else %}\
            %rs.read({{db_type}})
          {% end %})
        {% elsif value[:nilable] || value[:default] != nil %}\
          %rs.read(::Union({{value[:type]}} | Nil))
        {% else %}\
          %rs.read({{value[:type]}})
        {% end %}\
        if value.is_a?(Nil)
          {% if value[:nilable] %}\
            @{{key}} = nil
          {% elsif value[:default] != nil %}\
            @{{key}} = {{value[:default]}}
          {% else %}\
            raise "rs.read({{ db_type }}{{ "?".id if value[:nilable]}}) returns nil for #{ Table }->#{ Query[{{index}}] }"
          {% end %}\ 
        else
          @{{key}} = value.as({{value[:type]}})
        end
        {% index = index+1 %}\
      {% end %}\
    end
    
    @@%select_begin = "SELECT {{ 
        mapping.map{ |key,value|
          if value[:query] != nil
            value[:query].id
          else
            "#{ table_name }.#{ (value[:query] || value[:key] || key).id }".id
          end
        }.join(",").id
      }} FROM {{ table_name }}"

    def set_attributes(args : Hash(String, Object)) : Nil
      {% for key, value in mapping %}\
        {% type_id = value[:type].id %}\
        if val=args["{{key.id}}"]?
          begin
          {% if db_type = value[:internal_type] %}\
            {% if value[:type].has_attribute?(Flags.id) %}\
              raise "Not implemented for flags {{key.id}}: #{ val }"
            {% else %}\
              {% if value[:nilable] %}\
                @{{key.id}} = {{value[:type]}}.new(val)
              {% else %}\
                @{{key.id}} = {{value[:type]}}.new(val)
              {% end %}\
            {% end %}\
          {% elsif type_id == Int16.id %}\
            @{{key.id}} = val.is_a?(String) ? val.to_i16(strict: false) : val.is_a?(Int32) ? val.to_i16 : val.as(Int16)
          {% elsif type_id == Int32.id %}\
            @{{key.id}} = val.is_a?(String) ? val.to_i32(strict: false) : val.as(Int32)
          {% elsif type_id == Int64.id %}\
            @{{key.id}} = val.is_a?(String) ? val.to_i64(strict: false) : val.as(Int64)
          {% elsif type_id == Float32.id %}\
            @{{key.id}} = val.is_a?(String) ? val.to_f32(strict: false) : val.is_a?(Float64) ? val.to_f32 : val.as(Float32)
          {% elsif type_id == Float64.id %}\
            @{{key.id}} = val.is_a?(String) ? val.to_f64(strict: false) : val.as(Float64)
          {% elsif type_id == Bool.id %}\
            @{{key.id}} = TRUE_BOOLS.includes?(val)
          {% elsif type_id == Time.id %}\
            if val.is_a?(Time)
              @{{key.id}} = val.in(Granite.settings.default_timezone)
            else
              @{{key.id}} = Time.parse(val, Granite::DATETIME_FORMAT, Granite.settings.default_timezone)
            end
          {% elsif type_id == String.id %}\
            @{{key.id}} = val
          {% else %}\
            @{{key.id}} = val.to_s
            {% raise "unexpected type: #{key}: #{value}" %}
          {% end %}\

          {% if value[:nilable] %}\
          rescue
            @{{key.id}} = nil
          end
          {% else %}\
          rescue %msg
            raise "'#{val}' is invalid: #{%msg}"
          end
          {% end %}\
        end
      {% end %}\
    end

    {%
      without_id = {} of String => Nil
      params_insert = [] of String
      params_update = [] of String
      sets = [] of String
      inserts_count = 0
      updates_count = 0 
    %}\
    {% for key, value in mapping %}\
      {% if !["id".id, "created_at".id].includes?(key.id) && !value[:query] %}
        {% if key.id == "updated_at".id %}\
          {%
          updates_count = updates_count + 1
          sets << "updated_at=$#{updates_count}"
          params_update << "Time.utc"
          %}\
        {% else %}\
          {%
          inserts_count = inserts_count + 1
          updates_count = updates_count + 1
          value[:dollars] = "$#{inserts_count}".id
          without_id[key.id] = value
          p = value[:internal_type] ? "(v=@#{key}) && v.value".id : "@#{key}".id
          params_insert << p
          params_update << p
          sets << "#{ (value[:key] || key).id }=$#{updates_count}"
          %}\
        {% end %}\
      {% end %}\
    {% end %}\

    def self.all(*args) : Array({{ type_name }})
      ret = [] of {{ type_name }}
      {% if debug %}print "{{type_name}}.all "; %enterTime = Time.utc{% end %}
      if args.empty?
        get_connection.query_each(@@%select_begin){ |rs| ret.push {{ type_name }}.new(rs) }
      elsif args.size == 1
        get_connection.query_all("#{@@%select_begin} #{ args[0]? }"){ |rs| ret.push {{ type_name }}.new(rs) }
      elsif args.size == 2
        get_connection.query_all("#{@@%select_begin} #{ args[0]? }", args.last?){ |rs| ret.push {{ type_name }}.new(rs) }
      else
        raise "too many args #{args}!"
      end
      {% if debug %}\
        puts "#{ ((Time.utc - %enterTime).nanoseconds.to_f/1000000).to_s.colorize :green }ms: #{ret.size} #{ args }"
      {% end %}\
      ret
    end

    def self.one?(*args) : {{ type_name }}?
      {% if debug %}print "{{type_name}}.one? "; %enterTime = Time.utc{% end %}
      ret = if args.empty?
        get_connection.query_one("#{@@%select_begin} LIMIT 1"){ |rs| {{ type_name }}.new(rs) }
      elsif args.size == 1
        get_connection.query_one("#{@@%select_begin} #{ args[0]? } LIMIT 1"){ |rs| {{ type_name }}.new(rs) }
      elsif args.size == 2
        get_connection.query_one("#{@@%select_begin} #{ args[0]? } LIMIT 1", args.last?){ |rs| {{ type_name }}.new(rs) }
      else
        raise "too many args #{args}!"
      end
      {% if debug %}\
        puts "#{ ((Time.utc - %enterTime).nanoseconds.to_f/1000000).to_s.colorize :green }ms: #{ ret ? "1" : "0" } #{ args }"
      {% end %}\
      ret
    rescue e
      raise e if e.to_s != "no rows"
      nil
    end

    def self.one(*args) : {{ type_name }}
      self.one?(*args) || raise "{{@type}}.one: no row! #{args[0]?}"
    end

    def self.size(*args) : Int32
      # get_connection.query_one("SELECT COUNT(*) FROM {{table_name}}", as: Int32)
      size = if args.empty?
        get_connection.query_one("SELECT COUNT(*) FROM {{table_name}}", as: Int32|Int64)
      elsif args.size == 1
        get_connection.query_one("SELECT COUNT(*) FROM {{table_name}} #{ args[0]? }", as: Int32|Int64)
      elsif args.size == 2
        get_connection.query_one("SELECT COUNT(*) FROM {{table_name}} #{ args[0]? }", args[1]?, as: Int32|Int64)
      else
        raise "too many args #{args}!"
      end
      size.is_a?(Int32) ? size : size.to_i
    end

    def self.empty?
      self.size == 0
    end

    def self.clear
      get_connection.exec("DELETE FROM {{table_name}}")
      nil
    end

    {% if primaryName %}\
      def self.find(row_id)
        self.one?("WHERE id=$1", row_id)
      end

      def destroy
        {% if debug %}
          print "{{type_name}}.destroy id=#{ @id } "
          raise "@{{primaryName}} is nil" unless @{{primaryName}}
        {% end %}
        get_connection.exec("DELETE FROM {{table_name}} WHERE {{primaryName}}=$1", @{{primaryName}})
        # @{{primaryName}} = nil
        {% if debug %}
          puts "done".colorize :green
        {% end %}
        nil
      end

      macro find!(row_id)
        {{type_name}}.one?("WHERE id=$1", \{{row_id}}.to_i{{"16".id if primaryType.id==Int16.id }}{{"64".id if primaryType.id==Int64.id }}) || raise "{{@type}} not found!"
      end
    {% else %}\
      def destroy
        {% if debug %} print "{{type_name}}.destroy"  {% end %}
        get_connection.exec("DELETE FROM {{table_name}} WHERE {{ sets.join(" AND ").id }}", [{{ params_update.splat }}])
        {% if debug %}
          puts "done".colorize :green
        {% end %}
        nil
      end
    {% end %}\
    # search row by content of variable, using variable name for querying database 
    # variable name and database field name must match
    # ``` 
    # name = "Smith"
    # Users.oneBy?(name) # => Users | Nil
    # ```
    macro oneBy?(arg)
      {{type_name}}.one?("WHERE \{\{ arg.id.underscore }}=$1", \{\{arg}})
    end

    macro where(arg)
      {{type_name}}.all("WHERE \{\{ arg.id.underscore }}=$1", \{\{arg}})
    end

    {% if primaryName %}\
      {% params_update << "@id" %}\
      def insert : {{primaryType}}
        {% if debug %}print "{{type_name}}.insert "; %enterTime = Time.utc{% end %}
        @{{primaryName}} = get_connection.query_one("INSERT INTO {{table_name}} ({{ without_id.map{|k,v| (v[:key] || k) }.join(",").id }})
          VALUES ({{ without_id.map{|k,v| v[:dollars] }.join(",").id }})
          RETURNING {{primaryName}}", {{ params_insert.join(",").id }}, as: {{primaryType}})

        {% if debug %}\
          puts "#{ (Time.utc - %enterTime).nanoseconds.to_f/1000000 }ms: #{ @{{primaryName}} }"
        {% end %}\
        @{{primaryName}} || raise "zero {{primaryName}}"
      end

      # when no need for autogenerated id
      def push
        {% if debug %}print "{{type_name}}.insert "; %enterTime = Time.utc{% end %}
        get_connection.exec("INSERT INTO {{table_name}} ({{ without_id.map{|k,v| (v[:key] || k) }.join(",").id }})
          VALUES ({{ without_id.map{|k,v| v[:dollars] }.join(",").id }})", {{ params_insert.join(",").id }})

        {% if debug %}\
          puts "#{ (Time.utc - %enterTime).nanoseconds.to_f/1000000 }ms: #{ @{{primaryName}} }"
        {% end %}\
        nil
      end

      def insert_with_id
        {% params_insert.unshift "@id" %}\
        {% if debug %}print "{{type_name}}.insert_with_id "; %enterTime = Time.utc{% end %} 
        get_connection.exec("INSERT INTO {{table_name}} (id,{{ without_id.map{|k,v| (v[:key] || k) }.join(",").id }})
          VALUES ({{ without_id.map{|k,v| v[:dollars] }.join(",").id }}, ${{inserts_count+1}})", {{ params_insert.join(",").id }})

        {% if debug %}\
          puts "#{ (Time.utc - %enterTime).nanoseconds.to_f/1000000 }ms: #{ @{{primaryName}} }"
        {% end %}\
        nil
      end

      def update
        {% if debug %}print "{{type_name}}.update "; %enterTime = Time.utc{% end %} 
        get_connection.exec("UPDATE {{table_name}} SET {{ sets.join(",").id }}
          WHERE {{primaryName}}=${{updates_count+1}}", {{ params_update.join(",").id }})
        {% if debug %}\
          puts "#{ (Time.utc - %enterTime).nanoseconds.to_f/1000000 }ms: #{ @{{primaryName}} }"
        {% end %}\
        nil
      end
    {% else %}\
      def insert : Nil
        {% if debug %}print "{{type_name}}.insert "; %enterTime = Time.utc{% end %} 
        get_connection.exec("INSERT INTO {{table_name}} ({{ without_id.map{|k,v| (v[:key] || k) }.join(",").id }})
          VALUES ({{ without_id.map{|k,v| v[:dollars] }.join(",").id }})",
          {{ params_insert.join(",").id }})

        {% if debug %}\
          puts "#{ (Time.utc - %enterTime).nanoseconds.to_f/1000000 }ms"
        {% end %}\
      end
    {% end %}\

    {% if primaryName %}\
      def save!
        if @{{primaryName}}
          self.update
        else
          self.insert
        end
      end
    {% else %}\
      def save!
        self.insert
        nil
      end
    {% end %}\
  end
end

spawn do
  begin 
    nextUp = Time.utc # just now
    loop do
      if (timeCurrent = Time.utc) >= nextUp
        # print "MacroOrm is connection alive? "
        MacroOrm.db.exec "SELECT version();"
        # puts "yes"
      end
      while nextUp <= timeCurrent
        nextUp += 420.seconds
      end
      sleep (nextUp - Time.utc)
    rescue ex
      puts "MacroOrm: #{ ex }".colorize(:red).mode(:bold)
      if tx = MacroOrmTransactional.tx
        tx.close
        MacroOrmTransactional.tx = nil
      end
      MacroOrm.disconnect
    end
  end
end