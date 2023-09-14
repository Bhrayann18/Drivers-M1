/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.cltech.middleware.drivers.app;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.text.Segment;
import net.cltech.middleware.domain.ConnectionConfiguration;
import net.cltech.middleware.domain.Order;
import net.cltech.middleware.domain.Test;
import net.cltech.middleware.interfaces.Driver;
import net.cltech.middleware.interfaces.DriverManagerListener;
import net.cltech.middleware.interfaces.MLLPConstants;
import net.cltech.middleware.tools.Tools;
import org.apache.poi.hssf.util.CellReference;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

/**
 * Interfaz del equipo Malugmi
 *
 * @version 1.0.0
 * @author bcortes
 * @since 11-09-2023
 * @see Creación
 */
public class AnalyzerDriver implements Driver
{

    private String connectionID;
    private String connectionName;
    private String option1, option2, option3, option4;
    private boolean updateConfig;
    private boolean isRunning;
    private DriverManagerListener listener;
    private boolean isQC, isLoadSheet, isMicrobiology, withImages;
    private String buffer;
    private boolean isQuery;
    private final String QRY_MSH = "MSH|^~\\&|{connectionID}|{connectionName}|||{date}||QRY^Q02|1|P|2.5.1||||||ASCII|||"
            + MLLPConstants.CR;
    private final String QRY_QRD = "QRD|{date}|R|D|3|||RD|{order}|OTH|||T|" + MLLPConstants.CR;
    private final String QRY_QRF = "QRF||||||||||" + MLLPConstants.CR;
    private final String ORU_MSH = "MSH|^~\\&|{connectionID}|{connectionName}|||{date}||ORU^R01|1|P|2.5.1||||||ASCII|||"
            + MLLPConstants.CR;
    private final String ORU_PID = "PID|1" + MLLPConstants.CR;
    private final String ORU_OBR = "OBR|1|{order}|||||||||||||||||||||||||||||||||||||||||||{QC}|||" + MLLPConstants.CR;
    private final String ORU_OBX = "OBX|{index}|NM||{test}|{result}|||||F|||||||||{dateResult}" + MLLPConstants.CR;
    private final SimpleDateFormat yyyyMMddHHmmss;
    private ConnectionConfiguration connectionConfiguration;
    private ConnectionConfiguration connectionConfigurationTemp;
    private int index;
    private String hl7Message;
    private char state;
    private StringBuilder sendString;
    private Order order;
    private final String HEADER = "H|\\^&||PSWD|Maglumi 1000|||||Lis||P|E1394-97|20100319" + MLLPConstants.CR;
    private final String PATIENT = "P|1" + MLLPConstants.CR;
    private final String ORDER = "O|{index}|{order}||{test}|R" + MLLPConstants.CR;
    private final String ENDLINE = "L|1|{flag}" + MLLPConstants.CR;
    private String temString2;
    private boolean isInit, isScanFiles;
    private Timer timer;

    /**
     * constructor de la clase
     */
    public AnalyzerDriver()
    {
        connectionID = null;
        isScanFiles = false;
        isInit = false;
        connectionName = null;
        temString2 = "";
        option1 = null;
        listener = null;
        sendString = null;
        buffer = "";
        isQuery = false;
        yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
    }

    /**
     * Recibe las tramas de analizador
     *
     * @param stream trama
     */
    @Override
    public void receiveTCPMessage(String stream)
    {
        buffer += stream;
        if (buffer.contains(MLLPConstants.ENQ)
                || buffer.contains(MLLPConstants.ACK)
                || buffer.contains(MLLPConstants.EOT)
                || buffer.contains(MLLPConstants.NAK))
        {
            listener.registerTrace("[ANALYZER] : " + Tools.convertString(buffer));
            String trace = buffer;
            buffer = "";
            analyzerTrace(trace);
        } else if (buffer.contains(MLLPConstants.ETX))
        {
            listener.registerTrace("[ANALYZER] : " + Tools.convertString(buffer));
            String trace = buffer;
            buffer = "";
            try
            {
                temString2 += trace.substring(1, trace.lastIndexOf("" + MLLPConstants.ETX));
                String temDataTrace = temString2;
                temString2 = "";
                String rows[] = temDataTrace.split("" + MLLPConstants.CR);
                for (String row : rows)
                {
                    listener.registerTrace("[ROW] : " + Tools.convertString(row));
                    analyzerTrace(row + MLLPConstants.CR);
                }
            } finally
            {
                listener.sendTCPMessage("" + MLLPConstants.ACK);
            }
        }
    }

    /**
     * analiza las tramas que lelgan del analizador dependiendo de su
     * composicion
     *
     * @param trace segemnto de trama astm
     *
     */
    private void analyzerTrace(String trace)
    {
        if (trace.contains(MLLPConstants.ENQ))
        { // Pregunta desde el equipo
            listener.sendTCPMessage(MLLPConstants.ACK);
        } else if (trace.contains(String.valueOf(MLLPConstants.NAK)))
        {
            switch (state)
            {
                case 'P':
                    state = 'H';
                    break;
                case 'O':
                    state = 'P';
                    break;
                case 'L':
                    state = 'O';
                    break;
                case 'F':
                    state = 'L';
                    break;
            }
            listener.sendTCPMessage(MLLPConstants.ENQ);
        } else if (trace.contains(MLLPConstants.ACK))
        { // Equipo envia respuesta positiva

            if (state == 'H')
            {
                state = 'S';
                sendPosition();
            } else if (state == 'S')
            {
                listener.sendTCPMessage(MLLPConstants.EOT);
                state = 'E';
                listener.registerTrace(
                        "ORDER [ " + (order != null ? order.getOrder() : "") + "] HAS BEEN SENT TO ANALYZER");
                order = null;
                sendString = null;
            }
        } else if (trace.contains(MLLPConstants.EOT))
        { // Finaliza el envio de bloque el analizador
            isRunning = true;
            if (isQuery)
            { // Si es query
                updateConfiguration();
                String hl7Response = listener.queryOrder(hl7Message);
                state = 'H';
                listener.registerTrace("HL7 RESPONSE [" + hl7Response + "]");
                if (hl7Response != null && !hl7Response.isEmpty())
                {
                    createPatient(hl7Response);
                    listener.sendTCPMessage(MLLPConstants.ENQ);
                } else
                {
                    listener.registerTrace("ORDER TO ANALYZER NOT FOUND");
                }
            }
        } else if (trace.contains(MLLPConstants.CR))
        { // Fin de envio de cada segmento
            analyzerSegment(trace);
        }
    }

    /**
     * analiza los segmentos de trama astm para armar el mensaje hl7 que se
     * envia al middleware
     *
     * @param segment segmento de trama astm
     */
    private void analyzerSegment(String segment)
    {
        String fields[] = segment.split("\\|");
        switch (segment.substring(0, 1))
        {
            case "H": // Cabecera
                index = 1;
                hl7Message = "";
                String msh = ORU_MSH;
                msh = msh.replace("{connectionID}", connectionID);
                msh = msh.replace("{connectionName}", connectionName);
                msh = msh.replace("{date}", yyyyMMddHHmmss.format(new Date()));
                hl7Message += msh;
                String pid = ORU_PID;
                hl7Message += pid;
//                listener.registerTrace("hl7a" + hl7Message );
                break;
            case "O": // Orden
                String orderR = fields[2];
                if (orderR.contains("^"))
                {
                    orderR = orderR.substring(0, orderR.indexOf("^"));
                }
                listener.registerTrace("ORDER [" + orderR + "]");
                if (!orderR.isEmpty())
                {
                    String obr = ORU_OBR;
                    obr = obr.replace("{QC}", "");
                    obr = obr.replace("{order}", orderR);
                    hl7Message += obr;
                }
                break;
            case "R": // Resultado
                isQuery = false;
                String obx = ORU_OBX;
                String test = fields[2].split("\\^")[3].trim();
                String result = fields[3].trim();
                String date = yyyyMMddHHmmss.format(new Date());
                listener.registerTrace("TEST [" + test + "] RESULT [" + result + "]");
                if (result != null)
                {
                    obx = obx.replace("{index}", String.valueOf(index));
                    obx = obx.replace("{test}", test);
                    obx = obx.replace("{result}", result);
                    obx = obx.replace("{dateResult}", date);
                    index++;
                    hl7Message += obx;
                }
                break;
            case "Q": // Query
                isQuery = true;
                hl7Message = "";
                String msh2 = QRY_MSH;
                msh2 = msh2.replace("{connectionID}", connectionID);
                msh2 = msh2.replace("{connectionName}", connectionName);
                msh2 = msh2.replace("{date}", yyyyMMddHHmmss.format(new Date()));
                hl7Message += msh2;
                String qrd = QRY_QRD;
                String qOrder = segment.split("\\|")[2].trim();
                if (qOrder.contains("^"))
                {
                    qOrder = qOrder.substring(qOrder.indexOf("^") + 1).trim();
                }
                listener.registerTrace("ORDER [" + qOrder + "]");
                qrd = qrd.replace("{date}", yyyyMMddHHmmss.format(new Date()));
                qrd = qrd.replace("{order}", qOrder);
                String qrf = QRY_QRF;
                hl7Message += qrd + qrf;
//                listener.registerTrace("hl7b" + hl7Message );
                break;
            case "L": // Fin de Mensaje
                if (!isQuery)
                { // Si es resultados
                    new UpdateWorker(hl7Message).start();
                }
                break;
        }
    }

    @Override
    public void setOption1(String option1)
    {
        this.option1 = option1;
    }

    @Override
    public void setOption2(String option2)
    {
        this.option2 = option2;
    }

    @Override
    public void setConnectionID(String connectionID)
    {
        this.connectionID = connectionID;
    }

    @Override
    public void setConnectionName(String connectionName)
    {
        this.connectionName = connectionName;
    }

    @Override
    public void setDriverManagerListener(DriverManagerListener listener)
    {
        this.listener = listener;
    }

    @Override
    public void setQC(boolean qc)
    {
        this.isQC = qc;
    }
    // </editor-fold>

    /**
     * invoca la creacion de la entidad Order
     *
     * @param hl7Response mensaje de respuesta del middleware
     *
     */
    private void createPatient(String hl7Response)
    {
        try
        {
            order = Tools.createOrder(hl7Response);
        } catch (Exception ex)
        {
            listener.registerError("ERROR", ex);
        }
    }

    /**
     * arma las tramas astm para el envio hacia el analizador
     *
     */
    private void sendPosition()
    {
        try
        {
            sendString = new StringBuilder();
            String header = HEADER;
            header = MLLPConstants.STX + header.replace("{currentDate}", yyyyMMddHHmmss.format(new Date()));
            sendString.append(header);

            String patient = PATIENT;
            sendString.append(patient);

            String end = ENDLINE;
            List<Test> tests = connectionConfiguration.getConfigTests(order.getTests());
            if (tests.isEmpty())
            {
                String orderQ = ORDER;
                orderQ = orderQ.replace("{order}", order.getOrder())
                        .replace("{test}", "");
                end = end.replace("{flag}", "I");
                sendString.append(orderQ).append(end);
            } else
            {
                int indexOrder = 1;
                for (Test test : tests)
                {
                    String orderQ = ORDER;
                    orderQ = orderQ.replace("{order}", order.getOrder())
                            .replace("{test}", "^^^" + test.getCodTestConnect())
                            .replace("{index}", String.valueOf(indexOrder));
                    sendString.append(orderQ);
                    indexOrder++;
                }
                end = end.replace("{flag}", "N");
                sendString.append(end).append(MLLPConstants.ETX).append(MLLPConstants.EOT);
            }
            listener.sendTCPMessage(String.valueOf(sendString));
        } catch (Exception ex)
        {
            String header = HEADER;
            String patient = PATIENT;
            String orderQ = ORDER;
            String end = ENDLINE;
            orderQ = orderQ.replace("{order}", order.getOrder()).replace("{test}", "");
            end = end.replace("{flag}", "I");
            sendString.append(MLLPConstants.STX).append(header).append(patient).append(orderQ).append(end)
                    .append(MLLPConstants.ETX);
            listener.sendTCPMessage(String.valueOf(sendString));
            listener.registerError("ERROR", ex);
            order = null;
        }
    }

    @Override
    public void setConnectionConfiguration(ConnectionConfiguration connectionConfiguration)
    {
        if (isRunning)
        {
            connectionConfigurationTemp = connectionConfiguration;
            updateConfig = true;
        } else
        {
            this.connectionConfiguration = connectionConfiguration;
        }

        if (!isInit)
        {
            init();
            isInit = true;
        }
    }

    /**
     * Actualiza la confirguación de la interfaz desde la configuración recibida
     * del servidor
     */
    private void updateConfiguration()
    {
        if (updateConfig)
        {
            if (connectionConfigurationTemp != null)
            {
                connectionConfiguration = connectionConfigurationTemp;
                connectionConfigurationTemp = null;
                updateConfig = false;
            }
        }
    }

    /**
     * Obtiene la ruta de archivos con resultadosQC para ejecutar el timer.
     *
     * @param option3 Ruta origen.
     */
    private void init()
    {
        if (!option3.isEmpty())
        {
            File resultsQc = new File(option3);
            FileFilter filter = new Filter();
            listener.registerTrace("[RESULTS QC PATH] : " + resultsQc.getAbsolutePath());
            if (resultsQc.exists())
            {
                timer = new Timer(true);
                timer.schedule(new ScanReultsQC(resultsQc, filter), 5000, 10000);
            }
        }
    }

    /**
     * Obtiene todas las filas de la hoja de excel
     *
     * @param file Archivo entrante.
     * @param resutlsPath Ruta de origen
     */
    private void readFile(File file, File resutlsPath) throws FileNotFoundException, InvalidFormatException
    {
        try
        {
            try (InputStream inp = new FileInputStream(file))
            {
                Workbook wb = WorkbookFactory.create(inp);
                Sheet sheet = wb.getSheetAt(0);
                DataFormatter formatter = new DataFormatter();
                Iterator<Row> rowIterator = sheet.iterator();
                proccesFile(formatter, rowIterator);
                inp.close();
            }
            deleteAndBacukp(file, resutlsPath);
        } catch (IOException e)
        {
            listener.registerTrace("Error leyendo el archivo QC : " + Arrays.toString(e.getStackTrace()));
        }
    }

    /**
     * se recorre cada fila hasta el final para obtener el control con sus
     * examenes y resultados
     *
     * @param file Archivo entrante.
     * @param resutlsPath Ruta de origen
     * @param formatter métodos para formatear el valor almacenado en una celda
     * @Param rowIterator Celdas para recorridas
     */
    private void proccesFile(DataFormatter formatter, Iterator<Row> rowIterator)
    {
        Row row;
        String cellP = "";
        while (rowIterator.hasNext())
        {
            StringBuilder hl7MessageQC = new StringBuilder();
            String msh = ORU_MSH;
            msh = msh.replace("{connectionID}", connectionID)
                    .replace("{connectionName}", connectionName)
                    .replace("{date}", yyyyMMddHHmmss.format(new Date()));
            hl7MessageQC.append(msh).append(ORU_PID);
            String control = null;
            int indexQC = 1;
            String hTest = "";
            row = rowIterator.next();
            // se obtiene las celdas por fila
            Iterator<Cell> cellIterator = row.cellIterator();
            Cell cell;
            // se recorre cada celda
            while (cellIterator.hasNext())
            {
                String result = "";
                cell = cellIterator.next();
                // se obtiene la celda en específico y se la imprime
                CellReference cellRef = new CellReference(row.getRowNum(), cell.getColumnIndex());
                if (cellRef.formatAsString().contains("$"))
                {
                    cellP = cellRef.formatAsString().replace("$", "");
                }
                if (option2.equals("Control") && cellP.contains("B") && !cellP.contains("Nombre de QC")
                        || option2.equals("Lote") && cellP.contains("C") && !cellP.contains("N.o de"))
                {
                    control = formatter.formatCellValue(cell).trim().replace(" ", "");
                    listener.registerTrace("CONTROL: " + control);
                    setOBR(control, hl7MessageQC);
                } else if ((cellP.contains("D") && !formatter.formatCellValue(cell).contains("Ensayo")))
                {
                    String test = formatter.formatCellValue(cell);
                    hTest = getHomologationTest(test);
                    listener.registerTrace("TEST: " + hTest);
                } else if ((cellP.contains("G") && !formatter.formatCellValue(cell).contains("Conc")))
                {
                    result = formatter.formatCellValue(cell);
                    listener.registerTrace("RESULT: " + result);
                    setOBX(hTest, hl7MessageQC, indexQC, result);
                }
            }
            if (hl7MessageQC.toString().contains("OBX") && hl7MessageQC.toString().contains("OBR"))
            {
                sendQCRessults(hl7MessageQC.toString());
            }
        }
    }

    /**
     * Se crea el mensaje obr con QC obtenido
     *
     * @param hl7MessageQC StringBuilder ocn el mensaje de resultados a enviar a
     * Middleware.
     * @param control Control obtenido del archivo de resultados
     */
    private void setOBR(String control, StringBuilder hl7MessageQC)
    {
        if (control != null && !control.isEmpty())
        {
            String obr = ORU_OBR;
            hl7MessageQC.append(obr.replace("{order}", control).replace("{QC}", "1"));
        }
    }

    /**
     * Se busca el examen en la homologacion del driver
     *
     * @param test Examen del control
     * @return Si el examen fue encontrado sino retorna NOT FOUND
     */
    private String getHomologationTest(String test)
    {
        for (Test test1 : connectionConfiguration.getTests())
        {

            if (test1.getCodTestConnect().equals(test))
            {
                return test1.getCodTestConnect();
            }
        }
        return test.concat(" NOT FOUND");
    }

    /**
     * Se crea el mensaje obx con el examen y resultados obtenidos del archivos
     *
     * @param hl7MessageQC StringBuilder ocn el mensaje de resultados a enviar a
     * Middleware.
     * @param hTest Examen del control
     * @param result resultado del control
     * @param indexQC indice del obx
     */
    private void setOBX(String hTest, StringBuilder hl7MessageQC, int indexQC, String result)
    {
        if (!hTest.isEmpty() && !hTest.contains("NOT FOUND") && !result.isEmpty())
        {
            String obx = ORU_OBX;
            hl7MessageQC.append(obx.replace("{index}", String.valueOf(indexQC))
                    .replace("{test}", hTest)
                    .replace("{result}", result)
                    .replace("{dateResult}", yyyyMMddHHmmss.format(new Date())));
            indexQC++;
        }
    }

    /**
     * Se crea un backUp del archivo en la raiz de la carpeta en backUp y se
     * elimina el archivo
     *
     * @param file archivo a eliminar
     * @param resutlsPath ruta de origen
     */
    private void deleteAndBacukp(File file, File resutlsPath)
    {
        backUp(resutlsPath.getAbsolutePath() + System.getProperty("file.separator") + file.getName(),
                resutlsPath.getAbsolutePath() + System.getProperty("file.separator") + "BACKUP"
                + System.getProperty("file.separator") + file.getName());
    }

    /**
     * Realiza el backup de los archivos obtenidos.
     *
     * @param fromFile Ruta origen.
     * @param toFile Ruta destino.
     */
    private void backUp(String fromFile, String toFile)
    {
        File origin = new File(fromFile);
        File destination = new File(toFile);
        if (origin.exists())
        {
            try
            {
                origin.renameTo(destination);
            } catch (Exception e)
            {
                listener.registerError("Error moviendo el archivo", e);
            }
        }
    }

    private class ScanReultsQC extends TimerTask
    {

        File resutlsPath;
        FileFilter filter;

        public ScanReultsQC(File resutlsPath, FileFilter filter)
        {
            this.resutlsPath = resutlsPath;
            this.filter = filter;
        }

        @Override
        public void run()
        {
            if (!isScanFiles)
            {
                try
                {
                    verifiFiles(resutlsPath, filter);
                } catch (InvalidFormatException ex)
                {
                    Logger.getLogger(AnalyzerDriver.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /**
     * Verifica que la ruta tenga archivos con la extencion xls
     *
     * @param resutlsPath Ruta origen.
     * @param filter filtro con la extencion de los archivos
     */
    private void verifiFiles(File resutlsPath, FileFilter filter) throws InvalidFormatException
    {
        File files[] = resutlsPath.listFiles(filter);
        listener.registerTrace("FILES FOUND: " + files.length);
        if (files != null)
        {
            isScanFiles = true;
            for (File file : files)
            {
                try
                {
                    readFile(file, resutlsPath);
                } catch (FileNotFoundException ex)
                {
                    listener.registerTrace("Error archivo no encontrado: " + ex.getStackTrace());
                }
            }
            isScanFiles = false;
        }
    }

    public void sendQCRessults(String hl7MessageR)
    {
        listener.registerTrace("[REQUEST] : " + Tools.convertString(hl7MessageR));
        if (hl7MessageR != null && !hl7MessageR.isEmpty())
        {
            String hl7Response = listener.sendResult(hl7MessageR);
            listener.registerTrace("[RESPONSE] : " + hl7Response);
        } else
        {
            listener.registerTrace("No llegaron resultados");
        }
    }

    /**
     * clase para el envio de resultados hacia el middleware
     *
     */
    class UpdateWorker extends Thread
    {

        private String hl7MessageR;

        public UpdateWorker(String hl7Message)
        {
            this.hl7MessageR = hl7Message;
        }

        @Override
        public void run()
        {
            try
            {
                listener.registerTrace("[REQUEST] : " + Tools.convertString(hl7MessageR));
                if (hl7MessageR != null && !hl7MessageR.isEmpty())
                {
                    String hl7Response = listener.sendResult(hl7MessageR);
                    listener.registerTrace("[RESPONSE] : " + hl7Response);
                } else
                {
                    listener.registerTrace("No llegaron resultados");
                }
            } catch (Exception ex)
            {
                listener.registerError("ERROR : ", ex);
            } finally
            {
                done();
            }
        }

        private void done()
        {
            hl7MessageR = null;
            isRunning = false;
        }
    }

    @Override
    public void setOption3(String option3)
    {
        this.option3 = option3;
    }

    @Override
    public void setOption4(String option4)
    {
        this.option4 = option4;
    }

    @Override
    public void sendOrder(List<String> list)
    {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
        // Tools | Templates.
    }

    @Override
    public void setLoadSheet(boolean isLoadSheet)
    {
        this.isLoadSheet = isLoadSheet;
    }

    @Override
    public void setMicrobiology(boolean isMicrobiology)
    {
        this.isMicrobiology = isMicrobiology;
    }

    public String getOption3()
    {
        return option3;
    }

    public String getOption4()
    {
        return option4;
    }

    public boolean isIsLoadSheet()
    {
        return isLoadSheet;
    }

    public boolean isIsMicrobiology()
    {
        return isMicrobiology;
    }

    public boolean isWithImages()
    {
        return withImages;
    }

    public void setWithImages(boolean withImages)
    {
        this.withImages = withImages;
    }

    @Override
    public void closeTasks()
    {
        timer.cancel();
    }
}
