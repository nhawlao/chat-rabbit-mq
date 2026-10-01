package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeoutException;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class Chat {

  private static String meu_usuario = "";
  private static volatile String usuario_desejado = ""; 

  private static String recebe_mensagem() {
      if (usuario_desejado.isEmpty()) {
          return "<< ";
      }
      return "@" + usuario_desejado + "<< ";
  }

  public static void cria_usuario() {
    Scanner recebe_nome = new Scanner(System.in);
    System.out.print("Insira seu usuario: ");
    meu_usuario = recebe_nome.nextLine().trim();
  }

  public static void main(String[] argv) throws Exception {

    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(
        Path.of("config", "rabbitmq.properties"), StandardCharsets.UTF_8)) {
      properties.load(reader);
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(properties.getProperty("rabbitmq.host")); // Alterar
    factory.setUsername(properties.getProperty("rabbitmq.user")); // Alterar
    factory.setPassword(properties.getProperty("rabbitmq.password")); // Alterar
    factory.setVirtualHost("/");

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    
    Scanner scanner = new Scanner(System.in);
    cria_usuario();

    String QUEUE_NAME = meu_usuario;
    channel.queueDeclare(QUEUE_NAME, false,   false,     false,       null);
    
    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
        throws IOException {

        String message = new String(body, "UTF-8");
        String[] parts = message.split("::", 2);

        if (parts.length < 2) {
          return;
        }

        String contato = parts[0];
        String mensagem = parts[1];

        String timestamp = new SimpleDateFormat("dd/MM/yyyy 'às' HH:mm:ss").format(new Date());

        System.out.println();
        System.out.println("(" + timestamp + ") @" + contato + " diz: " + mensagem);

        System.out.println(recebe_mensagem());
      }
    };

    channel.basicConsume(QUEUE_NAME, true,    consumer);

    System.out.print(recebe_mensagem());

    while (true) {
      String nova_mensagem = scanner.nextLine();

      if (nova_mensagem.startsWith("@")) {
        usuario_desejado = nova_mensagem.substring(1).trim();
        System.out.print(recebe_mensagem());
            
      } else if (nova_mensagem.equalsIgnoreCase("!sair")) {
        break;
            
      } else if (!usuario_desejado.isEmpty()) {
        channel.queueDeclare(usuario_desejado, false, false, false, null);

        String message = meu_usuario + "::" + nova_mensagem;
                
        channel.basicPublish("", usuario_desejado, null, message.getBytes("UTF-8"));
                
        System.out.print(recebe_mensagem()); 
            
      } else {
        System.out.println("Use @<usuario> para selecionar um destinatário.");
        System.out.print(recebe_mensagem());
      }
    }

    System.out.println("Encerrando o chat...");
    scanner.close();
    try {
      channel.close();
      connection.close();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
  }
}