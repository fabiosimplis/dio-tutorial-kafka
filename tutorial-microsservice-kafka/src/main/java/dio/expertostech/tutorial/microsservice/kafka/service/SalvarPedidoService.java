package dio.expertostech.tutorial.microsservice.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dio.expertostech.tutorial.microsservice.kafka.data.PedidoData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SalvarPedidoService {

    @KafkaListener(topics = "SalvarPedido", groupId = "MicrosservicoSalvaPedido")
    private void executar(ConsumerRecord<String, String> record){

        log.info("Chave = {}", record.key());
        log.info("Cabecalho = {}", record.headers());
        log.info("Particao = {}", record.partition());

        String dados = record.value();

        ObjectMapper mapper = new ObjectMapper();
        PedidoData pedido;

        try {
            pedido = mapper.readValue(dados, PedidoData.class);
        } catch (JsonProcessingException e) {
            log.error("Falha ao converter evento [dado={}]", dados, e);
            return;
        }

        log.info("Evento recebido = {}", pedido);

        //TODO gravar no banco de dados
        // Responder a fila que o pedido foi salvo
    }

    private void grava(PedidoData pedido){
        //TODO salvar no banco de dados
    }
}
;