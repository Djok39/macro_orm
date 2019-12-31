
module MacroOrmTransactional
  class_property tx : DB::TopLevelTransaction? = nil
  class_property tx_mutex = Mutex.new

  macro included
    def self.transaction
      {% if flag?(:timings) %} %txTimeBegin = Time.now {% end %}
      ::MacroOrmTransactional.tx_mutex.synchronize do
        MacroOrm.db.transaction do |tx|
          raise "::MacroOrmTransactional.tx must be nil" if ::MacroOrmTransactional.tx
          begin
            ::MacroOrmTransactional.tx = tx
            yield tx
          ensure
            ::MacroOrmTransactional.tx = nil
          end  
        end
      end
      {% if flag?(:timings) %} puts "{{@type}}.transaction runtime: #{ (Time.now - %txTimeBegin).total_milliseconds }ms".colorize(:yellow) {% end %}
    end
  end
end
