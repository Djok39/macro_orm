# TODO: 
# - has_many relations in join_all
# - nilable single fields in join_all
# - join through
module MacroOrmJoins
  RelTypes = { :many, :one, :parent, :custom }

  macro has(relation, model, fkey = nil, through = nil)
    {%  
      raise "relation expected as one of #{ RelTypes }" unless RelTypes.includes? relation
      raise "model expected as Path: #{ model }" unless model.is_a? Path
      raise "through expected as Path?: #{ through }" if through && !through.is_a? Path
      puts "#{@type}.has #{relation} #{model} relation have compilation problems on complex models, i recommend to avoid that." if relation != :parent && relation != :custom
      model = model.resolve
      through = through.resolve if through
      table = ((tn = @type.constant("Table")) && tn.id) || @type.name.underscore.id
      join_table = ((tn = model.constant("Table")) && tn.id) || model.name.underscore.id
      through_table = through ? (((tn = through.constant("Table")) && tn.id) || through.name.underscore.id) : nil
      assoc = @type.constant("Has")
    %}

    {% if fkey %}
      {% fkey = fkey.id %}
    {% elsif through_table %}
      {% fkey = "#{ relation.id == :many.id ? through_table : join_table }_id".id %}
    {% else %}
      {% fkey = "#{ relation.id == :many.id ? table : join_table }_id".id %}
    {% end %}

    {% puts "#{@type}(#{ table }) has #{ relation } #{ model }(#{ join_table }) fkey: #{ fkey } through: #{ through }".id if false %}

    {% if assoc %}
      {% Has << { relation, model, join_table.symbolize, fkey.symbolize, through } %}
    {% else %}
      Has = [{ {{relation}}, {{model}}, {{join_table.symbolize}}, {{fkey.symbolize}}, {{through}} }]
    {% end %}
  end

  macro has_many(model, fkey = nil, through = nil)
    {{@type}}.has(:many, {{model}}, {{fkey}}, {{through}})
  end

  macro has_one(model, fkey = nil, through = nil)
    {{@type}}.has(:one, {{model}}, {{fkey}}, {{through}})
  end

  macro belongs_to(model, fkey = nil, through = nil)
    {{@type}}.has(:parent, {{model}}, {{fkey}}, {{through}})
  end
  # it returns array of tuple with affected models
  macro all_join(*args)
    {%
      table_name = @type.constant("Table").id
      assoc = @type.constant("Has")
      query_set = [] of String
      result_set = [] of String
      joins = [] of String
      clause = nil
      tail = [] of String
      sql = "".id
    %}\
    {% for arg in args %}\
      {% if clause %}\
        {% tail << arg %}\
      {% elsif arg.is_a?(Call) && arg.receiver.is_a?(Path) %}\
        {%     
          model = arg.receiver.resolve
          join_table = model.constant("Table").id
          fields = model.constant("Fields")
          queryes = model.constant("Query")
          # db_types = model.constant("StorageInternals")
          index = -1
          raise "#{ arg.name } not in #{ fields }" unless fields.find{ |x| index=index+1; x.id == arg.name }
          query = queryes[index]
          query_set << (query.is_a?(StringLiteral) ? queryes[index].id : "#{join_table}.#{queryes[index].id}".id)
          result_set << model.instance_vars.find{ |x| x.name==arg.name }.type
          joins << model
        %}\
      {% elsif arg.is_a?(Path) %}\
        {%
          model = arg.resolve
          raise "query fields not defined in #{arg}" unless fields = model.constant("Query")
          model_table = model.constant("Table").id
          query_set = query_set + fields.map{ |f| f.is_a?(StringLiteral) ? f.id : "#{model_table}.#{f.id}".id }
          result_set << model
          joins << model
        %}\
      {% elsif arg.is_a?(SymbolLiteral) %}\
        {% # process Symbols as main model fields
          model = @type
          table = model.constant("Table").id
          fields = model.constant("Fields")
          queryes = model.constant("Query")
          # db_types = model.constant("StorageInternals")
          index = -1
          raise "#{ arg.id } not in #{ fields }" unless fields.find{ |x| index=index+1; x.id == arg.id }
          query_set << "#{table}.#{queryes[index].id}".id
          result_set << model.instance_vars.find{ |x| x.name==arg.id }.type
          # joins << model
        %}\
      {% elsif arg.is_a?(Var) %}\
        {% 
          clause = "\#\{#{arg.id}\}".id
          # tail << arg 
        %}\
      {% elsif arg.is_a?(Call) && arg.receiver.is_a?(SymbolLiteral) %}\
        {% raise "#{arg} not supported" %}\
      {% elsif arg.is_a? StringLiteral %}\
        {% raise "only one StringLiteral clause allowed: #{arg}" if clause %}\
        {% clause = arg.id %}\
      {% else %}\
        {% pp arg
          raise "unexpected: #{ @type.name }.joining #{ arg.class_name }##{ arg }" %}\
        {% tail << arg %}\
      {% end %}\
    {% end %}\
    {%
      joins = joins.uniq
      sql = "SELECT ".id
    %}\
    {% unless query_set.empty? %}\
      {% sql = "#{sql}#{ query_set.splat }\n".id  %}\
    {% else %}\
      {% raise "nothing to join" %}\
    {% end %}\

    {% sql = "#{sql}FROM #{table_name} ".id %}\

    {% for model in joins %}\
      {% unless model.id == @type.id %}\
        {% through_table = nil %}\
        {% if rel = assoc.find{ |x| x[1].id == model.id } %}\
          {%
            rel_type = rel[0]
            rel_model= rel[1]
            rel_table= rel[2].id
            rel_fkey = rel[3].id
            rel_through= rel[4]
            rel_through= rel_through.resolve if rel_through.is_a? Path
            through_table = ((tn = rel_through.constant("Table")) && tn.id) || rel_through.name.underscore.id if rel_through
          %}\
        {% else %}\
          {%rel_model = model
            rel_type = :parent
            rel_table= ((tn = rel_model.constant("Table")) && tn.id) || rel_model.name.underscore.id
            rel_fkey = "#{ rel_table }_id".id
            rel_through=nil
          %}\
        {% end %}\
        {% puts "R.#{ @type }: has #{ rel_type.id } #{ rel_model} #{ rel_table.id }.#{ rel_fkey.id } through: #{ rel_through }"  %}
        {% if rel_type.id == :many.id %}\
          raise "not implemented #{ rel_type }"
        {% elsif rel_type.id == :one.id %}\
          {% puts "USING ONE RELATION #{rel_model}.#{rel_fkey}"
            t2 = through_table ? through_table : table_name
            sql = "#{sql}INNER JOIN #{rel_table} ON #{ t2 }.#{rel_fkey}=#{rel_table}.#{rel_fkey}\n".id  
          %}\
        {% elsif rel_type.id != :custom.id %}\
          {%
            t1 = rel_type.id == :many.id ? rel_table : table_name
            t2 = through_table ? through_table : t1
            sql = "#{sql}INNER JOIN #{rel_table} ON #{ t2 }.#{rel_fkey}=#{rel_table}.id\n".id  
          %}\
        {% end %}\
      {% end %}\
    {% end %}\

    {% if clause %}\
      {% sql = "#{sql}#{ clause }".id  %}\
    {% end %}\

    {% if flag?(:sqldebug) %}\
      puts "{{ sql }}".colorize :dark_gray
    {% end %}\
    {%
      params = [] of String
      params.push tail.size==1 ? tail[0] : tail unless tail.empty?
      push_tuple = [] of String
    %}\
    {% raise "params:\n#{args}\nstop: #{ @type.name }.all \"#{ sql }\" tail: #{ tail }, reqs: #{ query_set }, joins: #{ joins.uniq }" if false %}
    {% for result_type in result_set %}\
      {% if result_type.methods.find{ |x| x.name == "initialize" && x.args.size==1 && x.args[0].restriction.id == "DB::ResultSet".id } %}\
        {% push_tuple << "#{result_type}.new(rs)".id %}\
      {% elsif result_type.class.methods.find{ |x| x.name == "from_rs" } %}\
        {% push_tuple << "#{result_type}.from_rs(rs)".id %}\
      {% else %}\
        {% push_tuple << "rs.read(#{result_type})".id %}\
      {% end %}\
    {% end %}\

    {% if flag?(:timings) %}
      %measurePoint = Time.now
      %first = true
    {% end %}
    ret = [] of Tuple({{ result_set.splat }})
    MacroOrm.get_connection.query_each "{{sql}}", {{params.splat}} do |rs|
      {% if flag?(:timings) %}\
        if %first
          print "{{@type.name}}.first #{ ((Time.now - %measurePoint).nanoseconds.to_f/1000000).to_s.colorize :green }ms: {{result_set.splat}}"
          %first = false
        end
      {% end %}\

      ret << { {{push_tuple.splat}} }
    end
    {% if flag?(:timings) %}\
      puts " total #{ ((Time.now - %measurePoint).nanoseconds.to_f/1000000).to_s.colorize :green }ms: #{ ret.size }"
    {% end %}\
    ret
  end
end

# just garbage
# pp "Fields"
# pp model.constant("Query")
# pp "Call path: #{ arg.receiver.names }"
# last_init = arg.receiver.resolve.methods.select{ |m| m.name.includes? "initialize" }.last