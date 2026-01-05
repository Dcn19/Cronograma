using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Cronograma.Controllers
{
    [ApiController]
    [Route("api/relatorios")]
    public class RelatoriosController : ControllerBase
    {
        private readonly string _connString;

        // 0 = não iniciou, 1 = inicializando, 2 = pronto
        private static int _dbReady = 0;

        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        public RelatoriosController(IConfiguration configuration)
        {
            _connString =
                configuration.GetConnectionString("Default")
                ?? configuration["ConnectionStrings:Default"]
                ?? Environment.GetEnvironmentVariable("POSTGRES_CONNECTION")
                ?? Environment.GetEnvironmentVariable("DATABASE_URL")
                ?? throw new InvalidOperationException(
                    "ConnectionString não encontrada. Configure ConnectionStrings:Default no appsettings.json ou defina POSTGRES_CONNECTION/DATABASE_URL."
                );
        }

        // =========================
        //  MODELOS (JSON COMPLETO)
        // =========================

        public sealed class RelatorioItemDto
        {
            public DateTime? Data { get; set; }
            public string Periodo { get; set; } = "";
            public string Atividade { get; set; } = "";
            public decimal? Horas { get; set; }
            public string Status { get; set; } = "";
        }

        public sealed class RelatorioDadosDto
        {
            public DateTime? SemanaInicio { get; set; }
            public DateTime? SemanaFim { get; set; }

            // ✅ data referência escolhida na tela
            public DateTime? DataReferencia { get; set; }

            public string Colaborador { get; set; } = "";
            public string Setor { get; set; } = "";
            public string Supervisor { get; set; } = "";

            public string Projeto { get; set; } = "";
            public string Local { get; set; } = "";

            public string ResumoSemana { get; set; } = "";
            public string Observacoes { get; set; } = "";

            public List<RelatorioItemDto> Itens { get; set; } = new List<RelatorioItemDto>();

            // ✅ trava de edição (persistida no jsonb)
            public bool Finalizado { get; set; } = false;
        }

        public sealed class RelatorioCriarRequest
        {
            public string Colaborador { get; set; } = "";
            public DateTime Semana { get; set; } // início da semana (ex: segunda-feira)
            public DateTime? DataReferencia { get; set; }
            public string? Titulo { get; set; }
            public bool ForcarNovo { get; set; } = false;
        }

        public sealed class RelatorioUpdateRequest
        {
            public RelatorioDadosDto Dados { get; set; } = new RelatorioDadosDto();
            public string? Titulo { get; set; }
        }

        // =========================
        //  MODELOS: TREE (SIDEBAR)
        // =========================

        public sealed class RelatorioTreeResponse
        {
            public List<RelatorioTreeColaboradorNode> Colaboradores { get; set; } = new List<RelatorioTreeColaboradorNode>();
        }

        public sealed class RelatorioTreeColaboradorNode
        {
            public string Colaborador { get; set; } = "";
            public List<RelatorioTreeSemanaNode> Semanas { get; set; } = new List<RelatorioTreeSemanaNode>();
        }

        public sealed class RelatorioTreeSemanaNode
        {
            public string SemanaInicio { get; set; } = ""; // yyyy-MM-dd
            public string SemanaFim { get; set; } = "";    // yyyy-MM-dd
            public List<RelatorioTreeItemNode> Relatorios { get; set; } = new List<RelatorioTreeItemNode>();
        }

        public sealed class RelatorioTreeItemNode
        {
            public Guid Id { get; set; }
            public DateTime CriadoEm { get; set; }
            public DateTime AtualizadoEm { get; set; }

            public string Titulo { get; set; } = "";
            public string Url { get; set; } = "";

            // ✅ para abrir o HTML com ref correta
            public string? DataReferencia { get; set; } // yyyy-MM-dd | null

            // ✅ sidebar precisa disso para esconder os "⋯"
            public bool Finalizado { get; set; }

            // ✅ campos para busca/filtro no front (somente estes 4)
            public string Setor { get; set; } = "";
            public string Supervisor { get; set; } = "";
            public string Local { get; set; } = "";
            public string Projeto { get; set; } = "";
        }

        // =========================
        //  MODELOS: SUGESTÕES (AUTOCOMPLETE)
        // =========================

        public sealed class RelatoriosSugestoesResponse
        {
            public string Campo { get; set; } = "";       // setor | supervisor | projeto | local
            public string Colaborador { get; set; } = "";
            public string? Query { get; set; }            // q
            public string Fonte { get; set; } = "recent"; // recent | all
            public List<string> Itens { get; set; } = new List<string>();
        }

        // =========================
        //  MODELOS: SEARCH (CONTEÚDO)
        // =========================

        public sealed class RelatoriosSearchResponse
        {
            public string? Colaborador { get; set; }
            public string Query { get; set; } = "";
            public int Limit { get; set; }
            public int Offset { get; set; }
            public int Total { get; set; }
            public List<RelatorioSearchItemNode> Itens { get; set; } = new List<RelatorioSearchItemNode>();
        }

        public sealed class RelatorioSearchItemNode
        {
            public Guid Id { get; set; }
            public string Colaborador { get; set; } = "";
            public string SemanaInicio { get; set; } = ""; // yyyy-MM-dd
            public string SemanaFim { get; set; } = "";    // yyyy-MM-dd
            public string? DataReferencia { get; set; }    // yyyy-MM-dd | null

            public DateTime CriadoEm { get; set; }
            public DateTime AtualizadoEm { get; set; }

            public string Titulo { get; set; } = "";
            public string Url { get; set; } = "";
            public bool Finalizado { get; set; }

            // campos do json (pra mostrar contexto)
            public string Setor { get; set; } = "";
            public string Supervisor { get; set; } = "";
            public string Local { get; set; } = "";
            public string Projeto { get; set; } = "";

            // snippet simples pra UI
            public string Snippet { get; set; } = "";
            public string SnippetFonte { get; set; } = ""; // titulo | projeto | local | setor | supervisor | resumoSemana | observacoes | itens
        }

        // =========================
        //  ENDPOINTS
        // =========================

        [HttpPost("criar")]
        public async Task<IActionResult> Criar([FromBody] RelatorioCriarRequest req)
        {
            if (req == null)
                return BadRequest(new { message = "Body inválido." });

            if (string.IsNullOrWhiteSpace(req.Colaborador))
                return BadRequest(new { message = "Colaborador é obrigatório." });

            if (req.Semana == default)
                return BadRequest(new { message = "Semana é obrigatória (ex: 2025-12-15)." });

            await EnsureDbAsync();

            var colaborador = req.Colaborador.Trim();
            var semana = req.Semana.Date;

            var dataReferencia = (req.DataReferencia.HasValue && req.DataReferencia.Value != default)
                ? req.DataReferencia.Value.Date
                : semana;

            var tituloReq = (req.Titulo ?? "").Trim();
            var forcarNovo = req.ForcarNovo;

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            if (!forcarNovo)
            {
                const string sqlFindLatest = @"
                    SELECT id, titulo, atualizado_em,
                           CASE
                               WHEN (dados ? 'dataReferencia') AND NULLIF(dados->>'dataReferencia', '') IS NOT NULL
                                   THEN (dados->>'dataReferencia')::date
                               ELSE NULL
                           END AS data_referencia
                    FROM relatorio_atividades_semanais
                    WHERE colaborador = @colaborador AND semana = @semana
                    ORDER BY atualizado_em DESC
                    LIMIT 1;
                ";

                await using (var cmdFind = new NpgsqlCommand(sqlFindLatest, conn))
                {
                    cmdFind.Parameters.AddWithValue("colaborador", colaborador);
                    cmdFind.Parameters.AddWithValue("semana", semana);

                    await using var reader = await cmdFind.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        var existingId = reader.GetGuid(0);
                        var existingTitulo = reader.IsDBNull(1) ? "" : reader.GetString(1);
                        var atualizadoEm = reader.GetDateTime(2);
                        var dataRefDb = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

                        var dataRefIso = (dataRefDb ?? dataReferencia).Date.ToString("yyyy-MM-dd");
                        var titleFinal = !string.IsNullOrWhiteSpace(existingTitulo) ? existingTitulo.Trim() : $"Relatório {dataRefIso}";
                        var url = BuildRelatorioUrl(existingId, titleFinal, dataRefIso);

                        return Ok(new
                        {
                            id = existingId,
                            colaborador,
                            semana = semana.ToString("yyyy-MM-dd"),
                            dataReferencia = dataRefIso,
                            titulo = existingTitulo,
                            atualizadoEm,
                            jaExistia = true,
                            url
                        });
                    }
                }
            }

            var id = Guid.NewGuid();

            var dados = CriarTemplate(colaborador, semana, dataReferencia);
            var dadosJson = JsonSerializer.Serialize(dados, _jsonOptions);

            const string sqlInsert = @"
                INSERT INTO relatorio_atividades_semanais (id, colaborador, semana, titulo, dados, criado_em, atualizado_em)
                VALUES (@id, @colaborador, @semana, NULLIF(@titulo, ''), @dados::jsonb, NOW(), NOW());
            ";

            await using (var cmdIns = new NpgsqlCommand(sqlInsert, conn))
            {
                cmdIns.Parameters.AddWithValue("id", id);
                cmdIns.Parameters.AddWithValue("colaborador", colaborador);
                cmdIns.Parameters.AddWithValue("semana", semana);
                cmdIns.Parameters.AddWithValue("titulo", tituloReq);
                cmdIns.Parameters.AddWithValue("dados", dadosJson);
                await cmdIns.ExecuteNonQueryAsync();
            }

            var dataRefIsoNew = dataReferencia.ToString("yyyy-MM-dd");
            var titleNew = !string.IsNullOrWhiteSpace(tituloReq) ? tituloReq.Trim() : $"Relatório {dataReferencia:dd/MM} {DateTime.Now:HH:mm}";
            var urlNew = BuildRelatorioUrl(id, titleNew, dataRefIsoNew);

            return Ok(new
            {
                id,
                colaborador,
                semana = semana.ToString("yyyy-MM-dd"),
                dataReferencia = dataRefIsoNew,
                titulo = string.IsNullOrWhiteSpace(tituloReq) ? "" : tituloReq,
                jaExistia = false,
                url = urlNew
            });
        }

        [HttpGet("{id:guid}")]
        public async Task<IActionResult> GetById([FromRoute] Guid id)
        {
            if (id == Guid.Empty)
                return BadRequest(new { message = "Id inválido." });

            await EnsureDbAsync();

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            const string sql = @"
                SELECT id, colaborador, semana, titulo, dados, criado_em, atualizado_em
                FROM relatorio_atividades_semanais
                WHERE id = @id
                LIMIT 1;
            ";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id", id);

            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return NotFound(new { message = "Relatório não encontrado." });

            var rid = reader.GetGuid(0);
            var colaborador = reader.GetString(1);
            var semana = reader.GetDateTime(2);
            var titulo = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var dadosJson = reader.GetString(4);
            var criadoEm = reader.GetDateTime(5);
            var atualizadoEm = reader.GetDateTime(6);

            RelatorioDadosDto dados;
            try
            {
                dados = JsonSerializer.Deserialize<RelatorioDadosDto>(dadosJson, _jsonOptions)
                        ?? CriarTemplate(colaborador, semana, semana.Date);
            }
            catch
            {
                dados = CriarTemplate(colaborador, semana, semana.Date);
            }

            NormalizarDados(dados, colaborador, semana);

            return Ok(new
            {
                id = rid,
                colaborador,
                semana = semana.ToString("yyyy-MM-dd"),
                titulo,
                criadoEm,
                atualizadoEm,
                dados
            });
        }

        /// <summary>
        /// ✅ REGRA: se já estiver finalizado, bloqueia alterações.
        /// </summary>
        [HttpPut("{id:guid}")]
        public async Task<IActionResult> PutById([FromRoute] Guid id, [FromBody] RelatorioUpdateRequest req)
        {
            if (id == Guid.Empty)
                return BadRequest(new { message = "Id inválido." });

            if (req == null || req.Dados == null)
                return BadRequest(new { message = "Body inválido. Envie { dados: {...} }" });

            await EnsureDbAsync();

            string colaboradorDb;
            DateTime semanaDb;
            bool finalizadoDb;

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            const string sqlFind = @"
                SELECT
                    colaborador,
                    semana,
                    CASE
                        WHEN (dados ? 'finalizado') AND NULLIF(dados->>'finalizado','') IS NOT NULL
                            THEN (dados->>'finalizado')::boolean
                        ELSE false
                    END AS finalizado
                FROM relatorio_atividades_semanais
                WHERE id = @id
                LIMIT 1;
            ";

            await using (var cmdFind = new NpgsqlCommand(sqlFind, conn))
            {
                cmdFind.Parameters.AddWithValue("id", id);

                await using var r = await cmdFind.ExecuteReaderAsync();
                if (!await r.ReadAsync())
                    return NotFound(new { message = "Relatório não encontrado." });

                colaboradorDb = r.GetString(0);
                semanaDb = r.GetDateTime(1);
                finalizadoDb = r.GetBoolean(2);
            }

            if (finalizadoDb)
                return Conflict(new { message = "Relatório finalizado. Não é permitido alterar." });

            NormalizarDados(req.Dados, colaboradorDb, semanaDb);

            var dadosJson = JsonSerializer.Serialize(req.Dados, _jsonOptions);
            var titulo = (req.Titulo ?? "").Trim();

            const string sqlUpdate = @"
                UPDATE relatorio_atividades_semanais
                SET dados = @dados::jsonb,
                    titulo = CASE WHEN @titulo_is_null THEN titulo ELSE NULLIF(@titulo, '') END,
                    atualizado_em = NOW()
                WHERE id = @id
                  AND NOT (
                    CASE
                        WHEN (dados ? 'finalizado') AND NULLIF(dados->>'finalizado','') IS NOT NULL
                            THEN (dados->>'finalizado')::boolean
                        ELSE false
                    END
                  );
            ";

            int rows;
            await using (var cmdUpd = new NpgsqlCommand(sqlUpdate, conn))
            {
                cmdUpd.Parameters.AddWithValue("id", id);
                cmdUpd.Parameters.AddWithValue("dados", dadosJson);

                var tituloIsNull = req.Titulo == null;
                cmdUpd.Parameters.AddWithValue("titulo_is_null", tituloIsNull);
                cmdUpd.Parameters.AddWithValue("titulo", titulo);

                rows = await cmdUpd.ExecuteNonQueryAsync();
            }

            if (rows == 0)
                return Conflict(new { message = "Relatório finalizado. Não é permitido alterar." });

            return Ok(new { message = "Relatório atualizado com sucesso." });
        }

        [HttpGet("lookup")]
        public async Task<IActionResult> Lookup(
            [FromQuery] string colaborador,
            [FromQuery] DateTime semana)
        {
            if (string.IsNullOrWhiteSpace(colaborador))
                return BadRequest(new { message = "Query 'colaborador' é obrigatória." });

            if (semana == default)
                return BadRequest(new { message = "Query 'semana' é obrigatória (ex: 2025-12-15)." });

            await EnsureDbAsync();

            var colab = colaborador.Trim();
            var sem = semana.Date;

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            const string sqlSelect = @"
                SELECT id, titulo, atualizado_em,
                       CASE
                           WHEN (dados ? 'dataReferencia') AND NULLIF(dados->>'dataReferencia', '') IS NOT NULL
                               THEN (dados->>'dataReferencia')::date
                           ELSE NULL
                       END AS data_referencia
                FROM relatorio_atividades_semanais
                WHERE colaborador = @colaborador AND semana = @semana
                ORDER BY atualizado_em DESC
                LIMIT 1;
            ";

            await using var cmdSel = new NpgsqlCommand(sqlSelect, conn);
            cmdSel.Parameters.AddWithValue("colaborador", colab);
            cmdSel.Parameters.AddWithValue("semana", sem);

            await using var reader = await cmdSel.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return NotFound(new { message = "Relatório não encontrado." });

            var id = reader.GetGuid(0);
            var titulo = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var atualizadoEm = reader.GetDateTime(2);
            var dataRefDb = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

            var dataRefIso = (dataRefDb ?? sem).Date.ToString("yyyy-MM-dd");
            var titleFinal = !string.IsNullOrWhiteSpace(titulo) ? titulo.Trim() : $"Relatório {dataRefIso}";
            var url = BuildRelatorioUrl(id, titleFinal, dataRefIso);

            return Ok(new
            {
                id,
                colaborador = colab,
                semana = sem.ToString("yyyy-MM-dd"),
                titulo,
                atualizadoEm,
                dataReferencia = dataRefIso,
                url
            });
        }

        [HttpGet]
        public async Task<IActionResult> Listar(
            [FromQuery] string colaborador,
            [FromQuery] DateTime? semana = null,
            [FromQuery] int limit = 50)
        {
            if (string.IsNullOrWhiteSpace(colaborador))
                return BadRequest(new { message = "Query 'colaborador' é obrigatória." });

            await EnsureDbAsync();

            if (limit <= 0) limit = 50;
            if (limit > 200) limit = 200;

            var colab = colaborador.Trim();

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            var whereSemana = semana.HasValue && semana.Value != default;

            var sql = @"
                SELECT id, semana, titulo, criado_em, atualizado_em,
                       CASE
                           WHEN (dados ? 'dataReferencia') AND NULLIF(dados->>'dataReferencia', '') IS NOT NULL
                               THEN (dados->>'dataReferencia')::date
                           ELSE NULL
                       END AS data_referencia
                FROM relatorio_atividades_semanais
                WHERE colaborador = @colaborador
            ";

            if (whereSemana)
                sql += " AND semana = @semana ";

            sql += @"
                ORDER BY semana DESC, atualizado_em DESC
                LIMIT @limit;
            ";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("colaborador", colab);
            if (whereSemana) cmd.Parameters.AddWithValue("semana", semana.Value.Date);
            cmd.Parameters.AddWithValue("limit", limit);

            var items = new List<object>();

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var id = reader.GetGuid(0);
                var sem = reader.GetDateTime(1);
                var titulo = reader.IsDBNull(2) ? "" : reader.GetString(2);
                var criadoEm = reader.GetDateTime(3);
                var atualizadoEm = reader.GetDateTime(4);
                var dataRefDb = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);

                var dataRefIso = (dataRefDb ?? sem).Date.ToString("yyyy-MM-dd");
                var titleFinal = !string.IsNullOrWhiteSpace(titulo) ? titulo.Trim() : $"Relatório {dataRefIso}";
                var url = BuildRelatorioUrl(id, titleFinal, dataRefIso);

                items.Add(new
                {
                    id,
                    semana = sem.ToString("yyyy-MM-dd"),
                    titulo,
                    criadoEm,
                    atualizadoEm,
                    dataReferencia = dataRefIso,
                    url
                });
            }

            return Ok(new
            {
                colaborador = colab,
                semana = whereSemana ? semana.Value.Date.ToString("yyyy-MM-dd") : null,
                total = items.Count,
                items
            });
        }

        /// <summary>
        /// TREE pro Sidebar
        /// GET /api/relatorios/tree?limit=1000
        /// GET /api/relatorios/tree?colaborador=Fulano&limit=1000  ✅ filtra por colaborador
        /// </summary>
        [HttpGet("tree")]
        public async Task<IActionResult> Tree(
            [FromQuery] int limit = 1000,
            [FromQuery] string? colaborador = null)
        {
            await EnsureDbAsync();

            if (limit <= 0) limit = 1000;
            if (limit > 5000) limit = 5000;

            var colabFilter = (colaborador ?? "").Trim(); // "" => sem filtro

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            const string sql = @"
                SELECT
                    id,
                    colaborador,
                    semana,
                    titulo,
                    criado_em,
                    atualizado_em,
                    CASE
                        WHEN (dados ? 'finalizado') AND NULLIF(dados->>'finalizado','') IS NOT NULL
                            THEN (dados->>'finalizado')::boolean
                        ELSE false
                    END AS finalizado,
                    CASE
                        WHEN (dados ? 'dataReferencia') AND NULLIF(dados->>'dataReferencia', '') IS NOT NULL
                            THEN (dados->>'dataReferencia')::date
                        ELSE NULL
                    END AS data_referencia,

                    -- ✅ campos de conteúdo para busca no front
                    COALESCE(NULLIF(BTRIM(dados->>'setor'), ''), '') AS setor,
                    COALESCE(NULLIF(BTRIM(dados->>'supervisor'), ''), '') AS supervisor,
                    COALESCE(NULLIF(BTRIM(dados->>'local'), ''), '') AS local,
                    COALESCE(NULLIF(BTRIM(dados->>'projeto'), ''), '') AS projeto

                FROM relatorio_atividades_semanais
                WHERE (@colaborador = '' OR colaborador = @colaborador)
                ORDER BY colaborador ASC, semana DESC, atualizado_em DESC
                LIMIT @limit;
            ";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("limit", limit);
            cmd.Parameters.AddWithValue("colaborador", colabFilter);

            var rows = new List<(Guid Id, string Colaborador, DateTime Semana, string Titulo, DateTime CriadoEm, DateTime AtualizadoEm, bool Finalizado, DateTime? DataReferencia, string Setor, string Supervisor, string Local, string Projeto)>();

            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    rows.Add((
                        reader.GetGuid(0),
                        reader.GetString(1),
                        reader.GetDateTime(2),
                        reader.IsDBNull(3) ? "" : reader.GetString(3),
                        reader.GetDateTime(4),
                        reader.GetDateTime(5),
                        reader.GetBoolean(6),
                        reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7),

                        reader.IsDBNull(8) ? "" : reader.GetString(8),
                        reader.IsDBNull(9) ? "" : reader.GetString(9),
                        reader.IsDBNull(10) ? "" : reader.GetString(10),
                        reader.IsDBNull(11) ? "" : reader.GetString(11)
                    ));
                }
            }

            var resp = new RelatorioTreeResponse();

            foreach (var colabGroup in rows.GroupBy(x => x.Colaborador).OrderBy(g => g.Key))
            {
                var colNode = new RelatorioTreeColaboradorNode
                {
                    Colaborador = colabGroup.Key
                };

                foreach (var semanaGroup in colabGroup.GroupBy(x => x.Semana.Date).OrderByDescending(g => g.Key))
                {
                    var semanaInicio = semanaGroup.Key;
                    var semanaFim = semanaInicio.AddDays(6);

                    var semNode = new RelatorioTreeSemanaNode
                    {
                        SemanaInicio = semanaInicio.ToString("yyyy-MM-dd"),
                        SemanaFim = semanaFim.ToString("yyyy-MM-dd")
                    };

                    foreach (var r in semanaGroup.OrderByDescending(x => x.AtualizadoEm))
                    {
                        var dataTitulo = (r.DataReferencia ?? r.Semana).Date;

                        var titulo = !string.IsNullOrWhiteSpace(r.Titulo)
                            ? r.Titulo.Trim()
                            : $"Relatório {dataTitulo:dd/MM} {r.CriadoEm:HH:mm}";

                        var dataRefIso = (r.DataReferencia ?? r.Semana).Date.ToString("yyyy-MM-dd");
                        var url = BuildRelatorioUrl(r.Id, titulo, dataRefIso);

                        semNode.Relatorios.Add(new RelatorioTreeItemNode
                        {
                            Id = r.Id,
                            CriadoEm = r.CriadoEm,
                            AtualizadoEm = r.AtualizadoEm,
                            Titulo = titulo,
                            Url = url,
                            DataReferencia = dataRefIso,
                            Finalizado = r.Finalizado,

                            Setor = r.Setor ?? "",
                            Supervisor = r.Supervisor ?? "",
                            Local = r.Local ?? "",
                            Projeto = r.Projeto ?? ""
                        });
                    }

                    colNode.Semanas.Add(semNode);
                }

                resp.Colaboradores.Add(colNode);
            }

            return Ok(resp);
        }

        /// <summary>
        /// ✅ BUSCA POR CONTEÚDO DO RELATÓRIO (jsonb)
        ///
        /// GET /api/relatorios/search?q=manutencao
        /// GET /api/relatorios/search?q=manutencao&colaborador=FULANO
        /// GET /api/relatorios/search?q=manutencao&limit=50&offset=0
        ///
        /// Busca em:
        /// - titulo (coluna)
        /// - setor/supervisor/projeto/local (json)
        /// - resumoSemana/observacoes (json)
        /// - itens[].atividade / itens[].status / itens[].periodo (json)
        /// </summary>
        [HttpGet("search")]
        public async Task<IActionResult> Search(
            [FromQuery] string q,
            [FromQuery] string? colaborador = null,
            [FromQuery] int limit = 30,
            [FromQuery] int offset = 0,
            CancellationToken ct = default)
        {
            var term = (q ?? "").Trim();
            if (string.IsNullOrWhiteSpace(term))
                return BadRequest(new { message = "Query 'q' é obrigatória." });

            // evita query pesada com 1 caractere
            if (term.Length < 2)
                return BadRequest(new { message = "Query 'q' muito curta. Use pelo menos 2 caracteres." });

            await EnsureDbAsync();

            if (limit <= 0) limit = 30;
            if (limit > 100) limit = 100;
            if (offset < 0) offset = 0;

            var colabFilter = (colaborador ?? "").Trim();

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync(ct);

            const string sql = @"
                SELECT
                    COUNT(*) OVER() AS total_count,

                    r.id,
                    r.colaborador,
                    r.semana,
                    r.titulo,
                    r.criado_em,
                    r.atualizado_em,

                    CASE
                        WHEN (r.dados ? 'finalizado') AND NULLIF(r.dados->>'finalizado','') IS NOT NULL
                            THEN (r.dados->>'finalizado')::boolean
                        ELSE false
                    END AS finalizado,

                    CASE
                        WHEN (r.dados ? 'dataReferencia') AND NULLIF(r.dados->>'dataReferencia', '') IS NOT NULL
                            THEN (r.dados->>'dataReferencia')::date
                        ELSE NULL
                    END AS data_referencia,

                    COALESCE(NULLIF(BTRIM(r.dados->>'setor'), ''), '') AS setor,
                    COALESCE(NULLIF(BTRIM(r.dados->>'supervisor'), ''), '') AS supervisor,
                    COALESCE(NULLIF(BTRIM(r.dados->>'local'), ''), '') AS local,
                    COALESCE(NULLIF(BTRIM(r.dados->>'projeto'), ''), '') AS projeto,

                    COALESCE(NULLIF(BTRIM(r.dados->>'resumoSemana'), ''), '') AS resumo_semana,
                    COALESCE(NULLIF(BTRIM(r.dados->>'observacoes'), ''), '') AS observacoes,

                    COALESCE(NULLIF(BTRIM(mi.elem->>'atividade'), ''), '') AS item_atividade,
                    COALESCE(NULLIF(BTRIM(mi.elem->>'status'), ''), '') AS item_status,
                    COALESCE(NULLIF(BTRIM(mi.elem->>'periodo'), ''), '') AS item_periodo

                FROM relatorio_atividades_semanais r

                LEFT JOIN LATERAL (
                    SELECT elem
                    FROM jsonb_array_elements(COALESCE(r.dados->'itens', '[]'::jsonb)) elem
                    WHERE
                        COALESCE(elem->>'atividade','') ILIKE ('%' || @q || '%')
                        OR COALESCE(elem->>'status','') ILIKE ('%' || @q || '%')
                        OR COALESCE(elem->>'periodo','') ILIKE ('%' || @q || '%')
                    LIMIT 1
                ) mi ON true

                WHERE
                    (@colaborador = '' OR r.colaborador = @colaborador)
                    AND (
                        COALESCE(r.titulo,'') ILIKE ('%' || @q || '%')
                        OR COALESCE(r.dados->>'setor','') ILIKE ('%' || @q || '%')
                        OR COALESCE(r.dados->>'supervisor','') ILIKE ('%' || @q || '%')
                        OR COALESCE(r.dados->>'local','') ILIKE ('%' || @q || '%')
                        OR COALESCE(r.dados->>'projeto','') ILIKE ('%' || @q || '%')
                        OR COALESCE(r.dados->>'resumoSemana','') ILIKE ('%' || @q || '%')
                        OR COALESCE(r.dados->>'observacoes','') ILIKE ('%' || @q || '%')
                        OR mi.elem IS NOT NULL
                    )

                ORDER BY r.atualizado_em DESC
                LIMIT @limit
                OFFSET @offset;
            ";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("q", term);
            cmd.Parameters.AddWithValue("colaborador", colabFilter);
            cmd.Parameters.AddWithValue("limit", limit);
            cmd.Parameters.AddWithValue("offset", offset);

            var resp = new RelatoriosSearchResponse
            {
                Colaborador = string.IsNullOrWhiteSpace(colabFilter) ? null : colabFilter,
                Query = term,
                Limit = limit,
                Offset = offset,
                Total = 0,
                Itens = new List<RelatorioSearchItemNode>()
            };

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var totalSet = false;

            while (await reader.ReadAsync(ct))
            {
                if (!totalSet)
                {
                    resp.Total = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                    totalSet = true;
                }

                var id = reader.GetGuid(1);
                var colab = reader.GetString(2);
                var semana = reader.GetDateTime(3).Date;
                var titulo = reader.IsDBNull(4) ? "" : (reader.GetString(4) ?? "");
                var criadoEm = reader.GetDateTime(5);
                var atualizadoEm = reader.GetDateTime(6);
                var finalizado = reader.GetBoolean(7);
                var dataRefDb = reader.IsDBNull(8) ? (DateTime?)null : reader.GetDateTime(8);

                var setor = reader.IsDBNull(9) ? "" : (reader.GetString(9) ?? "");
                var supervisor = reader.IsDBNull(10) ? "" : (reader.GetString(10) ?? "");
                var local = reader.IsDBNull(11) ? "" : (reader.GetString(11) ?? "");
                var projeto = reader.IsDBNull(12) ? "" : (reader.GetString(12) ?? "");

                var resumo = reader.IsDBNull(13) ? "" : (reader.GetString(13) ?? "");
                var obs = reader.IsDBNull(14) ? "" : (reader.GetString(14) ?? "");

                var itemAtividade = reader.IsDBNull(15) ? "" : (reader.GetString(15) ?? "");
                var itemStatus = reader.IsDBNull(16) ? "" : (reader.GetString(16) ?? "");
                var itemPeriodo = reader.IsDBNull(17) ? "" : (reader.GetString(17) ?? "");

                var semanaFim = semana.AddDays(6);

                var dataRefIso = (dataRefDb ?? semana).Date.ToString("yyyy-MM-dd");
                var tituloFinal = !string.IsNullOrWhiteSpace(titulo)
                    ? titulo.Trim()
                    : $"Relatório {dataRefIso}";

                var url = BuildRelatorioUrl(id, tituloFinal, dataRefIso);

                var (snippet, fonte) = BuildSearchSnippet(
                    q: term,
                    titulo: tituloFinal,
                    projeto: projeto,
                    local: local,
                    setor: setor,
                    supervisor: supervisor,
                    resumoSemana: resumo,
                    observacoes: obs,
                    itemAtividade: itemAtividade,
                    itemStatus: itemStatus,
                    itemPeriodo: itemPeriodo
                );

                resp.Itens.Add(new RelatorioSearchItemNode
                {
                    Id = id,
                    Colaborador = colab,
                    SemanaInicio = semana.ToString("yyyy-MM-dd"),
                    SemanaFim = semanaFim.ToString("yyyy-MM-dd"),
                    DataReferencia = dataRefIso,

                    CriadoEm = criadoEm,
                    AtualizadoEm = atualizadoEm,

                    Titulo = tituloFinal,
                    Url = url,
                    Finalizado = finalizado,

                    Setor = setor,
                    Supervisor = supervisor,
                    Local = local,
                    Projeto = projeto,

                    Snippet = snippet,
                    SnippetFonte = fonte
                });
            }

            // se não veio nada, o total_count não seta (window não tem linha)
            if (!totalSet)
                resp.Total = 0;

            return Ok(resp);
        }

        /// <summary>
        /// ✅ SUGESTÕES (AUTOCOMPLETE)
        ///
        /// Regras:
        /// - setor/supervisor: traz tudo do colaborador (com filtro q opcional)
        /// - projeto/local:
        ///   - sem q: últimos 2 meses
        ///   - com q: tenta últimos 2 meses; se vazio, fallback no histórico inteiro do colaborador
        /// </summary>
        [HttpGet("sugestoes")]
        public async Task<IActionResult> Sugestoes(
            [FromQuery] string colaborador,
            [FromQuery] string campo,
            [FromQuery] string? q = null,
            [FromQuery] int limit = 20,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(colaborador))
                return BadRequest(new { message = "Query 'colaborador' é obrigatória." });

            if (string.IsNullOrWhiteSpace(campo))
                return BadRequest(new { message = "Query 'campo' é obrigatória. Use: setor, supervisor, projeto, local." });

            await EnsureDbAsync();

            var colab = colaborador.Trim();
            var campoNorm = campo.Trim().ToLowerInvariant();
            var qNorm = string.IsNullOrWhiteSpace(q) ? null : q.Trim();

            if (limit <= 0) limit = 20;
            if (limit > 50) limit = 50;

            var jsonKey = campoNorm switch
            {
                "setor" => "setor",
                "supervisor" => "supervisor",
                "projeto" => "projeto",
                "local" => "local",
                _ => ""
            };

            if (string.IsNullOrWhiteSpace(jsonKey))
                return BadRequest(new { message = "Campo inválido. Use: setor, supervisor, projeto, local." });

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync(ct);

            if (campoNorm is "setor" or "supervisor")
            {
                var itensAll = await QueryDistinctJsonFieldAsync(
                    conn: conn,
                    colaborador: colab,
                    jsonKey: jsonKey,
                    onlyLast2Months: false,
                    q: qNorm,
                    limit: limit,
                    ct: ct);

                return Ok(new RelatoriosSugestoesResponse
                {
                    Campo = campoNorm,
                    Colaborador = colab,
                    Query = qNorm,
                    Fonte = "all",
                    Itens = itensAll
                });
            }

            if (qNorm == null)
            {
                var itensRecent = await QueryDistinctJsonFieldAsync(
                    conn: conn,
                    colaborador: colab,
                    jsonKey: jsonKey,
                    onlyLast2Months: true,
                    q: null,
                    limit: limit,
                    ct: ct);

                return Ok(new RelatoriosSugestoesResponse
                {
                    Campo = campoNorm,
                    Colaborador = colab,
                    Query = null,
                    Fonte = "recent",
                    Itens = itensRecent
                });
            }
            else
            {
                var itensRecentMatch = await QueryDistinctJsonFieldAsync(
                    conn: conn,
                    colaborador: colab,
                    jsonKey: jsonKey,
                    onlyLast2Months: true,
                    q: qNorm,
                    limit: limit,
                    ct: ct);

                if (itensRecentMatch.Count > 0)
                {
                    return Ok(new RelatoriosSugestoesResponse
                    {
                        Campo = campoNorm,
                        Colaborador = colab,
                        Query = qNorm,
                        Fonte = "recent",
                        Itens = itensRecentMatch
                    });
                }

                var itensAllMatch = await QueryDistinctJsonFieldAsync(
                    conn: conn,
                    colaborador: colab,
                    jsonKey: jsonKey,
                    onlyLast2Months: false,
                    q: qNorm,
                    limit: limit,
                    ct: ct);

                return Ok(new RelatoriosSugestoesResponse
                {
                    Campo = campoNorm,
                    Colaborador = colab,
                    Query = qNorm,
                    Fonte = "all",
                    Itens = itensAllMatch
                });
            }
        }

        [HttpDelete("{id:guid}")]
        public async Task<IActionResult> DeleteById([FromRoute] Guid id)
        {
            if (id == Guid.Empty)
                return BadRequest(new { message = "Id inválido." });

            await EnsureDbAsync();

            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();

            const string sql = @"
                DELETE FROM relatorio_atividades_semanais
                WHERE id = @id;
            ";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            if (rows == 0)
                return NotFound(new { message = "Relatório não encontrado para excluir." });

            return Ok(new { message = "Relatório excluído com sucesso." });
        }

        // =========================
        //  HELPERS
        // =========================

        private static string BuildRelatorioUrl(Guid id, string? title, string? refIso)
        {
            var url = $"/relatorio/index.html?id={id}";

            var t = (title ?? "").Trim();
            if (!string.IsNullOrWhiteSpace(t))
            {
                var enc = Uri.EscapeDataString(t);
                url += $"&title={enc}&titulo={enc}";
            }

            var r = (refIso ?? "").Trim();
            if (!string.IsNullOrWhiteSpace(r))
            {
                var encRef = Uri.EscapeDataString(r);
                url += $"&ref={encRef}";
            }

            return url;
        }

        private static RelatorioDadosDto CriarTemplate(string colaborador, DateTime semanaInicio, DateTime dataReferencia)
        {
            var semIni = semanaInicio.Date;
            var semFim = semIni.AddDays(6);
            var dataRef = dataReferencia == default ? semIni : dataReferencia.Date;

            return new RelatorioDadosDto
            {
                SemanaInicio = semIni,
                SemanaFim = semFim,
                DataReferencia = dataRef,

                Colaborador = colaborador,
                Setor = "",
                Supervisor = "",

                Projeto = "",
                Local = "",

                ResumoSemana = "",
                Observacoes = "",

                Finalizado = false,

                Itens = new List<RelatorioItemDto>
                {
                    new RelatorioItemDto
                    {
                        Data = dataRef,
                        Periodo = "",
                        Atividade = "",
                        Horas = 0,
                        Status = ""
                    }
                }
            };
        }

        private static void NormalizarDados(RelatorioDadosDto dados, string colaboradorDb, DateTime semanaDb)
        {
            if (dados == null) return;

            dados.Colaborador = string.IsNullOrWhiteSpace(dados.Colaborador) ? colaboradorDb : dados.Colaborador.Trim();

            var semIni = semanaDb.Date;
            dados.SemanaInicio = semIni;

            if (dados.SemanaFim == null || dados.SemanaFim == default)
                dados.SemanaFim = semIni.AddDays(6);
            else
                dados.SemanaFim = dados.SemanaFim.Value.Date;

            if (dados.DataReferencia == null || dados.DataReferencia == default)
                dados.DataReferencia = semIni;
            else
                dados.DataReferencia = dados.DataReferencia.Value.Date;

            dados.Setor = (dados.Setor ?? "").Trim();
            dados.Supervisor = (dados.Supervisor ?? "").Trim();
            dados.Projeto = (dados.Projeto ?? "").Trim();
            dados.Local = (dados.Local ?? "").Trim();
            dados.ResumoSemana = (dados.ResumoSemana ?? "").Trim();
            dados.Observacoes = (dados.Observacoes ?? "").Trim();

            if (dados.Itens == null)
                dados.Itens = new List<RelatorioItemDto>();

            if (dados.Itens.Count == 0)
            {
                dados.Itens.Add(new RelatorioItemDto
                {
                    Data = dados.DataReferencia,
                    Periodo = "",
                    Atividade = "",
                    Horas = 0,
                    Status = ""
                });
            }

            foreach (var item in dados.Itens)
            {
                if (item == null) continue;

                if (item.Data == null || item.Data == default)
                    item.Data = dados.DataReferencia;

                item.Data = item.Data?.Date;

                item.Periodo = (item.Periodo ?? "").Trim();
                item.Atividade = (item.Atividade ?? "").Trim();
                item.Status = (item.Status ?? "").Trim();

                if (item.Horas.HasValue)
                {
                    if (item.Horas.Value < 0) item.Horas = 0;
                    if (item.Horas.Value > 8) item.Horas = 8;
                }
            }
        }

        private static async Task<List<string>> QueryDistinctJsonFieldAsync(
            NpgsqlConnection conn,
            string colaborador,
            string jsonKey,
            bool onlyLast2Months,
            string? q,
            int limit,
            CancellationToken ct)
        {
            var sql = @"
                SELECT DISTINCT val
                FROM (
                    SELECT
                        NULLIF(BTRIM(dados->> @jsonKey), '') AS val,
                        COALESCE(atualizado_em, criado_em) AS dt
                    FROM relatorio_atividades_semanais
                    WHERE colaborador = @colaborador
                ) x
                WHERE x.val IS NOT NULL
            ";

            if (onlyLast2Months)
                sql += " AND x.dt >= (NOW() - INTERVAL '2 months') ";

            if (!string.IsNullOrWhiteSpace(q))
                sql += " AND x.val ILIKE ('%' || @q || '%') ";

            sql += @"
                ORDER BY x.val
                LIMIT @limit;
            ";

            var itens = new List<string>();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("colaborador", colaborador);
            cmd.Parameters.AddWithValue("jsonKey", jsonKey);
            cmd.Parameters.AddWithValue("limit", limit);

            if (!string.IsNullOrWhiteSpace(q))
                cmd.Parameters.AddWithValue("q", q!.Trim());

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                if (reader.IsDBNull(0)) continue;
                var v = (reader.GetString(0) ?? "").Trim();
                if (!string.IsNullOrWhiteSpace(v))
                    itens.Add(v);
            }

            return itens;
        }

        private static (string snippet, string fonte) BuildSearchSnippet(
            string q,
            string titulo,
            string projeto,
            string local,
            string setor,
            string supervisor,
            string resumoSemana,
            string observacoes,
            string itemAtividade,
            string itemStatus,
            string itemPeriodo)
        {
            var term = (q ?? "").Trim();
            if (string.IsNullOrWhiteSpace(term))
                return ("", "");

            // ordem: o que normalmente dá mais sentido pro usuário
            var checks = new List<(string fonte, string label, string text)>
            {
                ("titulo", "Título", titulo ?? ""),
                ("projeto", "Projeto", projeto ?? ""),
                ("local", "Local", local ?? ""),
                ("setor", "Setor", setor ?? ""),
                ("supervisor", "Supervisor", supervisor ?? ""),
                ("resumoSemana", "Resumo", resumoSemana ?? ""),
                ("observacoes", "Observações", observacoes ?? ""),
                ("itens", "Atividade", itemAtividade ?? ""),
                ("itens", "Status", itemStatus ?? ""),
                ("itens", "Período", itemPeriodo ?? ""),
            };

            foreach (var c in checks)
            {
                var s = (c.text ?? "").Trim();
                if (string.IsNullOrWhiteSpace(s)) continue;

                var idx = s.IndexOf(term, StringComparison.OrdinalIgnoreCase);
                if (idx < 0) continue;

                var excerpt = MakeExcerpt(s, term, 140);
                return ($"{c.label}: {excerpt}", c.fonte);
            }

            // fallback: se veio match por SQL mas não achou index (muito raro), mostra um pedaço qualquer
            var fallback = (resumoSemana ?? "").Trim();
            if (!string.IsNullOrWhiteSpace(fallback))
                return ($"Resumo: {MakeExcerpt(fallback, term, 140)}", "resumoSemana");

            return ("", "");
        }

        private static string MakeExcerpt(string text, string term, int maxLen)
        {
            var s = (text ?? "").Trim();
            if (string.IsNullOrWhiteSpace(s)) return "";

            if (maxLen < 40) maxLen = 40;

            var idx = s.IndexOf(term ?? "", StringComparison.OrdinalIgnoreCase);
            if (idx < 0)
            {
                if (s.Length <= maxLen) return s;
                return s.Substring(0, maxLen).TrimEnd() + "…";
            }

            var half = Math.Max(10, maxLen / 2);
            var start = Math.Max(0, idx - half);
            var end = Math.Min(s.Length, idx + (term?.Length ?? 0) + half);

            var slice = s.Substring(start, end - start).Trim();

            var prefix = start > 0 ? "…" : "";
            var suffix = end < s.Length ? "…" : "";

            // evita "……"
            return (prefix + slice + suffix).Replace("……", "…");
        }

        private async Task EnsureDbAsync()
        {
            if (Volatile.Read(ref _dbReady) == 2)
                return;

            if (Interlocked.CompareExchange(ref _dbReady, 1, 0) == 0)
            {
                try
                {
                    await using var conn = new NpgsqlConnection(_connString);
                    await conn.OpenAsync();

                    const string ddlCreate = @"
                        CREATE TABLE IF NOT EXISTS relatorio_atividades_semanais (
                            id uuid NOT NULL,
                            colaborador   text NOT NULL,
                            semana        date NOT NULL,
                            titulo        text NULL,
                            dados         jsonb NOT NULL,
                            criado_em     timestamp NOT NULL DEFAULT NOW(),
                            atualizado_em timestamp NOT NULL DEFAULT NOW(),
                            CONSTRAINT pk_relatorio_atividades_semanais_id PRIMARY KEY (id)
                        );
                    ";
                    await using (var cmd = new NpgsqlCommand(ddlCreate, conn))
                        await cmd.ExecuteNonQueryAsync();

                    const string ddlAlter = @"
                        ALTER TABLE relatorio_atividades_semanais
                        ADD COLUMN IF NOT EXISTS criado_em timestamp NOT NULL DEFAULT NOW();

                        ALTER TABLE relatorio_atividades_semanais
                        ADD COLUMN IF NOT EXISTS atualizado_em timestamp NOT NULL DEFAULT NOW();

                        ALTER TABLE relatorio_atividades_semanais
                        ADD COLUMN IF NOT EXISTS id uuid;

                        ALTER TABLE relatorio_atividades_semanais
                        ADD COLUMN IF NOT EXISTS titulo text;
                    ";
                    await using (var cmd = new NpgsqlCommand(ddlAlter, conn))
                        await cmd.ExecuteNonQueryAsync();

                    while (true)
                    {
                        const string sqlPick = @"
                            SELECT ctid, colaborador, semana
                            FROM relatorio_atividades_semanais
                            WHERE id IS NULL
                            LIMIT 200;
                        ";

                        var toFix = new List<(string Ctid, string Colab, DateTime Semana)>();

                        await using (var cmdPick = new NpgsqlCommand(sqlPick, conn))
                        await using (var reader = await cmdPick.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var ctid = reader.GetString(0);
                                var c = reader.GetString(1);
                                var s = reader.GetDateTime(2);
                                toFix.Add((ctid, c, s));
                            }
                        }

                        if (toFix.Count == 0)
                            break;

                        foreach (var row in toFix)
                        {
                            const string sqlUpd = @"
                                UPDATE relatorio_atividades_semanais
                                SET id = @id
                                WHERE ctid = @ctid;
                            ";
                            await using var cmdUpd = new NpgsqlCommand(sqlUpd, conn);
                            cmdUpd.Parameters.AddWithValue("id", Guid.NewGuid());
                            cmdUpd.Parameters.AddWithValue("ctid", row.Ctid);
                            await cmdUpd.ExecuteNonQueryAsync();
                        }
                    }

                    const string ddlNotNull = @"
                        ALTER TABLE relatorio_atividades_semanais
                        ALTER COLUMN id SET NOT NULL;
                    ";
                    await using (var cmd = new NpgsqlCommand(ddlNotNull, conn))
                        await cmd.ExecuteNonQueryAsync();

                    const string ddlDropUnique = @"
                        DO $$
                        BEGIN
                          IF EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conrelid = 'relatorio_atividades_semanais'::regclass
                              AND conname = 'uq_relatorio_atividades_semanais_colab_semana'
                          ) THEN
                            ALTER TABLE relatorio_atividades_semanais
                              DROP CONSTRAINT uq_relatorio_atividades_semanais_colab_semana;
                          END IF;
                        END $$;
                    ";
                    await using (var cmd = new NpgsqlCommand(ddlDropUnique, conn))
                        await cmd.ExecuteNonQueryAsync();

                    const string ddlPk = @"
                        DO $$
                        BEGIN
                          IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conrelid = 'relatorio_atividades_semanais'::regclass
                              AND contype = 'p'
                          ) THEN
                            ALTER TABLE relatorio_atividades_semanais
                              ADD CONSTRAINT pk_relatorio_atividades_semanais_id PRIMARY KEY (id);
                          END IF;
                        END $$;
                    ";
                    await using (var cmd = new NpgsqlCommand(ddlPk, conn))
                        await cmd.ExecuteNonQueryAsync();

                    const string ddlIndexes = @"
                        CREATE INDEX IF NOT EXISTS ix_relatorio_colaborador ON relatorio_atividades_semanais (colaborador);
                        CREATE INDEX IF NOT EXISTS ix_relatorio_semana ON relatorio_atividades_semanais (semana);
                        CREATE INDEX IF NOT EXISTS ix_relatorio_colab_semana_upd ON relatorio_atividades_semanais (colaborador, semana, atualizado_em DESC);
                        CREATE INDEX IF NOT EXISTS ix_relatorio_titulo ON relatorio_atividades_semanais (titulo);

                        -- ✅ ajuda bastante o /search quando filtra por colaborador + ordena por atualizado_em
                        CREATE INDEX IF NOT EXISTS ix_relatorio_colab_upd_desc ON relatorio_atividades_semanais (colaborador, atualizado_em DESC);
                        CREATE INDEX IF NOT EXISTS ix_relatorio_upd_desc ON relatorio_atividades_semanais (atualizado_em DESC);
                    ";
                    await using (var cmd = new NpgsqlCommand(ddlIndexes, conn))
                        await cmd.ExecuteNonQueryAsync();

                    Volatile.Write(ref _dbReady, 2);
                }
                catch
                {
                    Volatile.Write(ref _dbReady, 0);
                    throw;
                }

                return;
            }

            while (Volatile.Read(ref _dbReady) != 2)
                await Task.Delay(50);
        }
    }
}
