Imports System.Data
Imports System.Data.SqlClient
Imports System.Configuration
Imports System.IO
Imports System.Xml
Imports System.Text

Module modMain
    Private Const Q As String = Chr(34)
    Private WithEvents moCon As SqlConnection

    Private msConnectionString As String = ""
    Private miTimeOut As Int16 = 30

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="oArgs"></param>
    ''' <returns></returns>
    ''' <remarks>
    ''' Expected parameters:
    ''' IN/OUT - Determines whether an xml file should be imported into a table or a table should be exported to an xml</remarks>
    ''' *sqlfilename* - The name of a file containing a sql query to be executed either to export or import an xml file.
    ''' *xmlfilename* - The name of a file to be created with table contents (for export) or that contains data to be imported (for import)
    Public Function Main(ByVal oArgs() As String) As Integer
        Dim iRetCode As Integer = 0

        'SERVER DATABASE OUT C:\data\sql_toxml\mluser.sql C:\data\xml_out\mluser.xml
        'SERVER DATABASE IN C:\data\sql_todb\insert_MLUser.sql C:\data\xml_out\mluser.xml
        Dim lRetValue As Long = 0

        Dim sServerName As String = ""
        Dim sDatabaseName As String = ""
        Dim sDirection As String = ""
        Dim sSQLFileName As String = ""
        Dim sXMLFileName As String = ""

        Dim sUserID As String = ""
        Dim sPassword As String = ""

        For iArg = 0 To oArgs.Count - 1
            Select Case oArgs(iArg)
                Case "/S"
                    sServerName = oArgs(iArg + 1)

                Case "/D"
                    sDatabaseName = oArgs(iArg + 1)

                Case "/UID"
                    sUserID = oArgs(iArg + 1)

                Case "/PWD"
                    sPassword = oArgs(iArg + 1)

                Case "/X"
                    sDirection = oArgs(iArg + 1).ToUpper

                    If sDirection <> "I" And sDirection <> "O" Then
                        Console.WriteLine("Invalid value for /X parameter. " & sDirection & " is not valid and should be I or O")
                    End If

                Case "/SQL"
                    sSQLFileName = oArgs(iArg + 1)

                Case "/XML"
                    sXMLFileName = oArgs(iArg + 1)

                Case Else
                    Console.WriteLine("Invalid argument: " & oArgs(iArg))
                    iRetCode = -200
            End Select
        Next

        If iRetCode = 0 Then
            lRetValue = Execute( _
                sServerName:=sServerName, _
                sDatabase:=sDatabaseName, _
                sUserID:=sUserID, _
                sPassword:=sPassword, _
                sDirection:=sDirection, _
                sSQLFileName:=sSQLFileName, _
                sXMLFileName:=sXMLFileName)
        Else
            Console.WriteLine()
            Console.WriteLine("Valid arguments:")
            Console.WriteLine(" /S      Server name")
            Console.WriteLine(" /D      Database name")

            Console.WriteLine(" [/UID]  User ID")
            Console.WriteLine(" [/PWD]  Password")
            Console.WriteLine("         User ID and password are optional. If not provided, integrated authentication will be used.")

            Console.WriteLine(" /X      Direction (I = input or O = output)")
            Console.WriteLine(" /SQL    SQL Filename (stored procedure that will export xml or process imported xml).")
            Console.WriteLine(" /XML    XML file to be created (for exports) or consumed (for imports)")
            Console.WriteLine()
        End If

        Return lRetValue
    End Function

    Public Function Execute( _
                    ByVal sServerName As String, _
                    ByVal sDatabase As String, _
                    ByVal sUserID As String, _
                    ByVal sPassword As String, _
                    ByVal sDirection As String, _
                    ByVal sSQLFileName As String, _
                    ByVal sXMLFileName As String) As Integer

        Dim iRetValue As Integer = 0

        Dim sSQL As String = GetSQL(sSQLFileName)

        Dim oStopWatch As New Stopwatch
        oStopWatch.Start()
        Console.WriteLine("Starting: " & Now)

        If sUserID = "" Then
            msConnectionString = "Data Source=" & sServerName & _
                                ";Initial Catalog=" & sDatabase & _
                                ";Integrated Security=True"
        Else
            msConnectionString = "Data Source=" & sServerName & _
                                ";Initial Catalog=" & sDatabase & _
                                ";User Id=" & sUserID & _
                                ";Password=" & sPassword & _
                                ";"
        End If

        Console.WriteLine(msConnectionString)

        Dim sTimeOut As String = ConfigurationManager.AppSettings("DBCON_Timeout")
        If IsNumeric(sTimeOut) Then miTimeOut = sTimeOut

        Try
            Select Case sDirection.ToUpper
                Case "IN"
                    Dim oFile As New FileInfo(sSQLFileName)
                    ImportFromXML(Path.GetFileNameWithoutExtension(oFile.Name), sSQL, sXMLFileName)

                Case "OUT"
                    ExtractToXML(sSQL, sXMLFileName)
            End Select

        Catch oEX As Exception
            Console.WriteLine("An exception occurred:")
            Console.WriteLine("  Type: " & oEX.GetType.ToString)
            Console.WriteLine("  Message: " & oEX.Message)
            Console.WriteLine("  StackTrace: " & oEX.StackTrace.ToString)
        End Try

        Console.WriteLine("Ending: " & Now & " elapsed seconds: " & oStopWatch.Elapsed.TotalSeconds)

        Return iRetValue
    End Function

    Public Sub LogPrintStatements( _
        ByVal sender As Object, _
        ByVal e As SqlInfoMessageEventArgs) Handles moCon.InfoMessage

        Console.WriteLine(e.Message & vbCrLf)
    End Sub

    Private Function GetSQL(ByVal sSQLFileName As String) As String
        Dim sSQL As String = ""

        Dim oReader As New StreamReader(sSQLFileName)
        sSQL = oReader.ReadToEnd
        oReader.Close()
        oReader.Dispose()

        Return sSQL
    End Function

    Private Sub ImportFromXML( _
                    ByVal sProcName As String, _
                    ByVal sProcSQL As String, _
                    ByVal sFileName As String)


        Dim oReader As New StreamReader(sFileName, Encoding.UTF8)
        Dim sXML As String = oReader.ReadToEnd

        '--Replace encoding. All sql xml uses unicode (utf-16) encoding.
        sXML = Microsoft.VisualBasic.Strings.Replace( _
                sXML, _
                "encoding=" & Q & "utf-8" & Q, _
                "encoding=" & Q & "utf-16" & Q, _
                 CompareMethod.Text)

        oReader.Close()
        oReader.Dispose()

        '--Open connection
        moCon = New SqlConnection(msConnectionString)
        moCon.Open()

        '--Create temporary stored procedure
        Dim oCMD As New SqlCommand(sProcSQL, moCon)
        oCMD.CommandType = CommandType.Text
        oCMD.CommandTimeout = 0
        oCMD.ExecuteNonQuery()
        oCMD.Dispose()

        '--Execute temporary stored procedure
        oCMD = New SqlCommand("#" & sProcName, moCon)
        oCMD.CommandType = CommandType.StoredProcedure
        oCMD.CommandTimeout = 0

        Dim oXMLParm As New SqlClient.SqlParameter("@xml", sXML)
        oXMLParm.SqlDbType = SqlDbType.Xml
        oCMD.Parameters.Add(oXMLParm)
        oCMD.ExecuteNonQuery()

        oCMD.Dispose()
        moCon.Close()
        moCon.Dispose()
    End Sub

    Private Sub ExtractToXML( _
                    ByVal sQuery As String, _
                    ByVal sFileName As String)

        moCon = New SqlConnection(msConnectionString)
        moCon.Open()

        Dim oCMD As New SqlCommand(sQuery, moCon)
        oCMD.CommandTimeout = miTimeOut

        Dim oDR As SqlDataReader = oCMD.ExecuteReader(CommandBehavior.SequentialAccess)

        If oDR.Read Then
            SaveBlobToFile( _
                            oReader:=oDR, _
                            sColumnName:="xml", _
                            sFileName:=sFileName)
        End If

        oDR.Close()
        oCMD.Dispose()
        moCon.Close()
        moCon.Dispose()
    End Sub

    Private Function SaveBlobToFile( _
             ByVal oReader As SqlDataReader, _
             ByVal sColumnName As String, _
             ByVal sFileName As String) As Integer

        Dim iBufferSize As Integer = 100
        Dim oBuffer(iBufferSize - 1) As Char

        ' create file to hold the output
        Dim oFS As New FileStream(sFileName, FileMode.Create, FileAccess.Write)
        Dim oBW As New BinaryWriter(oFS)

        ' set the starting read position
        Dim iStartIndex As Integer = 0

        Dim lRetVal As Long

        ' read bytes into aryOutbyte byte array and retain number of bytes returned (lngRetrieval)
        lRetVal = oReader.GetChars(oReader.GetOrdinal(sColumnName), iStartIndex, oBuffer, 0, iBufferSize)

        ' continue reading and writing while there are bytes beyond the size of the buffer
        ' it is safer to read in chunks then to read all file at once
        Do While lRetVal = iBufferSize
            oBW.Write(oBuffer)
            oBW.Flush()

            '    ' reposition the start index to the end of the last buffer and fill the buffer
            iStartIndex += iBufferSize
            lRetVal = oReader.GetChars(oReader.GetOrdinal(sColumnName), iStartIndex, oBuffer, 0, iBufferSize)
        Loop

        ' write the remaining buffer
        If lRetVal > 0 Then
            oBW.Write(oBuffer, 0, lRetVal)
            oBW.Flush()         ' flush the buffer
        End If

        ' close the output file
        oBW.Close()
        oFS.Close()

        Return lRetVal
    End Function

End Module
