# TODO: write tests
# TODO: check/rewrite initialize(Float64), initialize(String)

require "./rate"
# require "lucky_record"
# , more = false
def gaussRound(num : Float64, precision = 8) : Float64 
  m = 10 ** precision
  n = ((precision!=0)? num * m : num)
  i = n.floor
  f = n - i
  e = 1e-8
  if (f > 0.5-e && f < 0.5+e)
    r = ((i % 2) == 0)? i : (i + 1)
  else
    r = n.round(0)
  end
  (precision!=0)? (r / m) : r
end

macro currencyMacro(name, precision)
  struct {{ name }}
    Precision = {{ precision }}
    Divider = (10.0 ** Precision)
    Multiplier = 1.0 / Divider
    MAX = Multiplier * Int64::MAX
    MIN = Multiplier * Int64::MIN
    # include Comparable(Price)
    include Comparable({{ name }})
    # include Comparable(Int64)
    include Comparable(Int32)
    include Comparable(Float64)

    property value : Int64
    def initialize(rs : ::DB::ResultSet)
      @value = rs.read(Int64)
    end
    def initialize(floatAmmount : Float64)
      if floatAmmount > MAX || floatAmmount < MIN
        raise "internal value overflow for #{floatAmmount}"
      end
      floatStr = "%0.#{Precision+1}f" % floatAmmount
      # dotIndex = floatStr.index '.'
      # посчитаем количество знаков после запятой.
      digitsAfterDot = nil.as Int32?
      floatStr.to_slice.reverse_each do |c|
        next if digitsAfterDot==nil && c==48 # пропускам нули
        break if c==46 # dot
        if digitsAfterDot
          digitsAfterDot+=1
        else
          digitsAfterDot=1
        end
      end
      digitsAfterDot=0 unless digitsAfterDot
      # puts "Precision #{digitsAfterDot} for #{floatStr}(#{floatAmmount})"
      if digitsAfterDot > Precision # округление применяем только если значащих цифр больше желаемой точности.
        floatStr = "%.#{Precision}f" % gaussRound(floatAmmount, Precision)
      end
      controlValue = {{ name }}.new(floatStr).value
      @value = controlValue # TODO: придумать более оптимальный способ.
      return
      # Этот код вносит погрешности...
      n = floatAmmount.floor
      f = floatAmmount - n
      @value = n.to_i64 * Divider.to_i64
      if f != 0.0
        @value += gaussRound(f * Divider, 0).to_i64
      end

      # @value = gaussRound(floatAmmount / Multiplier, 0).to_i64
      if @value != controlValue
        puts "Currency init fault: #{@value} != #{controlValue} for #{floatAmmount}".colorize(:red).mode(:bright)
        raise "so sad."
      end
      # puts "#{@value} vs #{floatAmmount}"
      # @value = gaussRound(floatAmmount / Multiplier, 0).to_i64
    end
    def initialize(str : String)
      chars = str.size
      raise "empty" if chars.zero?
      if dotIndex = str.index('.')
        nullsToAdd = Precision - (chars - dotIndex) + 1
        # puts "dotIndex=#{dotIndex}, nullsToAdd=#{nullsToAdd}"
      else
        nullsToAdd = Precision
      end
      if nullsToAdd<0
        str = str[0, chars+nullsToAdd]
      else
        nullsToAdd.times{ str+="0" }
      end
      @value = dotIndex ? str.delete('.').to_i64 : str.to_i64
      # firstPart, secondPart = str.split(".")
      # puts "first: #{firstPart}, second: #{secondPart}"
      # @value = firstPart.to_i64 * Divider.to_i64
      # if secondPart
      #   nullsToAdd = Precision - secondPart.size
      #   if nullsToAdd<0
      #     secondPart = secondPart[0, 8]
      #   else
      #     nullsToAdd.times{ secondPart+="0" }
      #   end
      #   puts "add #{nullsToAdd} nulls: #{secondPart}"
      #   @value += (secondPart.to_i64) 
      # end
    end
    def initialize(intAmmount : Int32)
      @value = intAmmount.to_i64 * Divider.to_i64
    end
    def initialize(value : Nil) : {{ name }}
      raise "what?"
      @value = 0_i64
      self
    end
    def self.from_i64(other : Int64)
      {{ name }}.new(other * Divider.to_i64)
    end
    def self.from_rs(rs)
      {{ name }}.new rs.read(Int64)
    end
    def self.from_rs(rs)
      if dbVal = rs.read(Int64?)
        {{ name }}.new dbVal
      else
        nil
      end
    end
    # def initialize(rawInt64 : Int64 = 0_i64)
    #   @value = rawInt64
    # end
    # Для bitshares
    def initialize(rawInt64 : Int64 = 0_i64, customPrecision : Int16 = Precision.to_i16)
      diff = (customPrecision-Precision)
      if diff.zero?
        @value = rawInt64
      elsif diff > 0
        @value = gaussRound(rawInt64 / 10.0**diff, 0).to_i64
      else
        @value = (rawInt64 / 10.0**diff).to_i64
      end
    end

    def to_s(io)
      io << ("%.#{Precision}f" % self.to_f)
    end
    def to_s
      ("%.#{Precision}f" % self.to_f)
    end
    def to_s(precision : Int16)
      ("%.#{precision}f" % self.to_f)
    end
    def to_json(json : JSON::Builder)
      json.string(self.to_s)
    end
    def to_f : Float64
      Multiplier * @value
    end
    def to_sx(io)
      io << ("%0#{Precision+1}i" % @value).insert(-1-Precision, ".")
    end

    def +(other : Float64 | Int32 | Int64 | {{ name }})
      if other.is_a?({{ name }})
        {{ name }}.new(@value + other.value)
      #elsif other.is_a?(Price) 
        # {{ name }}.new((@value * (Price::Divider / Currency::Divider).to_i64 + other.value)/(Price::Divider / Currency::Divider).to_i64 )
      elsif other.is_a?(Int64)
        {{ name }}.new(@value + (other * Divider.to_i64) )
      else
        self + {{ name }}.new(other)
      end
    end

    def -(other : Float64 | Int32 | Int64 | {{ name }})
      if other.is_a?({{ name }})
        {{ name }}.new(@value - other.value)
      elsif other.is_a?(Int64)
        {{ name }}.new(@value - (other * Divider.to_i64) )
      else
        self - {{ name }}.new(other)
      end
    end

    def *(other)
      if other.is_a?(Float64)
        {{ name }}.new(self.to_f * other)
      elsif other.is_a?(Int64 | Int32)
        {{ name }}.new(@value * other) # result of multiplication must be Int64 
      else
        self * other.to_f 
      end
    end

    def invert
      {{ name }}.new(1_i64) / self
    end

    def /(other)
      if other.is_a?(Price | Currency)
        {{ name }}.new(self.to_f / other.to_f)
      else
        {{ name }}.new(self.to_f / other)
      end
    end

    def self.zero : {{ name }}
      new(0_i64)
    end
    def self.max 
      new(Int64::MAX)
    end
    def self.min 
      new(Int64::MIN)
    end
    def zero?
      @value == 0_i64
    end
    def blank?
      false
    end
    def abs : {{ name }}
      @value < 0_i64 ? -self : self
    end
    def -
      {{ name }}.new(0_i64 - @value) 
    end
    def trunc!(precision : Int16); # Параметр - количество нулей, сохраняемых после запятой
      dropDigits = Precision - precision
      return if dropDigits <= 0  # достигнут предел точности
      # Количество отбрасываемых знаков
      Log.debug "Truncate #{self} to #{ {{ name}}.new(@value - (@value % (10 ** dropDigits))) }"
      @value -= @value % (10 ** dropDigits)
    end

    def trunc(precision : Int16) : {{ name }}
      dropDigits = Precision - precision
      return self if dropDigits <= 0
      Log.debug "Truncate #{self} to #{ {{ name}}.new(@value - (@value % (10 ** dropDigits))) }"
      {{ name }}.new(@value - (@value % (10 ** dropDigits)) )
    end

    def <=>(other)
      if other.is_a?({{ name }})
        if @value == other.value
          0
        elsif @value < other.value
          -1
        else
          1
        end
      # !!! raw value comparing
      # elsif other.is_a?(Int64)
      #   @value == other
      elsif other.is_a?(Int32|Float64)
        (self <=> {{ name }}.new(other))
      else
        raise "unexpected comparing"
      end
    end

    def same?(other)
      self == other
    end

    # module Lucky
    #   alias ColumnType = {{ name }}
    #   include LuckyRecord::Type

    #   def self.from_db!(value : {{name}})
    #     value
    #   end

    #   def self.to_db(value : {{name}})
    #     value.value # .to_s
    #   end

    #   def self.parse(value : String)
    #     SuccessfulCast({{name}}?).new(value.empty? ? nil.as({{name}}?) :  {{name}}.new(value))
    #   rescue
    #     FailedCast.new
    #   end

    #   def self.parse(value : {{name}})
    #     SuccessfulCast({{name}}).new value
    #   end

    #   def self.parse(value : Int64)
    #     SuccessfulCast({{name}}).new {{name}}.new(value)
    #   end

    #   class Criteria(T, V) < LuckyRecord::Criteria(T, V)
    #   end
    # end
  end
end

currencyMacro(Currency, 8)
# currencyMacro(Price, 10)
alias Price = Rate
# raise "not expected" if Price::Divider < Currency::Divider

class Sha1
  include Comparable(Sha1)
  getter value : Slice(UInt8)

  def value=(raw)
    raise "SHA1 must be 20 bytes long" if raw.size!=20
    @value = raw
  end

  def initialize(raw : Slice(UInt8))
    @value = raw
  end

  def initialize(str : String)
    @value = str.hexbytes
  end

  def initialize(rs : ::DB::ResultSet)
    @value = rs.read(Slice(UInt8))
  end
    
  def self.from_rs(rs)
    if dbVal = rs.read(Slice(UInt8))
      Sha1.new dbVal
    else
      nil
    end
  end

  def self.zero
    Sha1.new Slice(UInt8).new(20)
  end

  def to_s(io) : Nil
    io << @value.hexstring
  end

  def <=>(other)
    @value <=> other.value
  end
end