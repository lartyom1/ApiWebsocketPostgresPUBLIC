//This dictionary is used to reduce amount of data stored in Db
//On State Changed event only state Id and Time is pushed into Db
public class StateDictionary
{
    public List<DictionaryLine> ListData { get; set; }
}

public class DictionaryLine
{
    public int StateId { get; set; }

    public string ParameterOne { get; set; }
    public string FieldOne { get; set; }
    /*
     * --- *
     * --- *
    */
    public bool FieldTwo { get; set; }
    public bool FieldThree { get; set; }
}
