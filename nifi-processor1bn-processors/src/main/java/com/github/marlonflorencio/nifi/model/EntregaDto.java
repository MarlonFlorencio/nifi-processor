package com.github.marlonflorencio.nifi.model;

public class EntregaDto {

    public String endereco;
    public String numero;
    public String cidade;
    public String status;

    public EntregaDto(String endereco, String numero, String cidade, String status) {
        this.endereco = endereco;
        this.numero = numero;
        this.cidade = cidade;
        this.status = status;
    }

    public String getEndereco() {
        return endereco;
    }

    public void setEndereco(String endereco) {
        this.endereco = endereco;
    }

    public String getNumero() {
        return numero;
    }

    public void setNumero(String numero) {
        this.numero = numero;
    }

    public String getCidade() {
        return cidade;
    }

    public void setCidade(String cidade) {
        this.cidade = cidade;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
