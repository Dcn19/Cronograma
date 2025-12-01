using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cronograma.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MPXJ.Net;
using Npgsql;

// para não conflitar com System.Threading.Tasks.Task
using TaskEntity = MPXJ.Net.Task;

namespace Cronograma.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CronogramaController : ControllerBase
    {
        private const string ConnectionString =
            "Host=192.168.100.108;Port=5432;Username=postgres;Password=admin;Database=postgres";

        // ============================================================
        // Classe interna para montar árvore de tarefas
        // ============================================================
        private class TaskNode
        {
            public string? OutlineKey { get; set; }
            public string? ParentKey { get; set; }
            public Dictionary<string, object?> Data { get; } = new();
            public List<TaskNode> Children { get; } = new();
        }

        // ============================================================
        // POST api/cronograma/upload
        // Cria um novo projeto a partir de um .mpp
        // ============================================================
        [HttpPost("upload")]
        [Consumes("multipart/form-data")]
        [ProducesResponseType(typeof(ProjectResponseDto), StatusCodes.Status200OK)]
        public async Task<ActionResult<ProjectResponseDto>> UploadAsync(
            IFormFile file,
            [FromForm] string projectName,
            CancellationToken ct)
        {
            if (file == null || file.Length == 0)
                return BadRequest("Nenhum arquivo enviado.");

            var tempPath = Path.GetTempFileName();

            try
            {
                // Salva o .mpp temporariamente
                await using (var fs = System.IO.File.Create(tempPath))
                {
                    await file.CopyToAsync(fs, ct);
                }

                var reader = new UniversalProjectReader();
                var project = reader.Read(tempPath);

                // Nome vindo do usuário tem prioridade
                var finalProjectName =
                    !string.IsNullOrWhiteSpace(projectName)
                        ? projectName
                        : project.ProjectProperties?.Name
                          ?? project.ProjectProperties?.ProjectTitle
                          ?? file.FileName;

                // Lista de tarefas do projeto
                var tasks = project.Tasks.Cast<TaskEntity>().ToList();

                // Colunas fixas
                var columns = new List<ColumnDefinitionDto>
                {
                    new ColumnDefinitionDto { Key = "Name",  Label = "Nome da Tarefa",    Type = "string"   },
                    new ColumnDefinitionDto { Key = "Duration", Label = "Duração",       Type = "duration" },
                    new ColumnDefinitionDto { Key = "Start", Label = "Início",            Type = "date"    },
                    new ColumnDefinitionDto { Key = "Finish", Label = "Término",          Type = "date"    },
                    new ColumnDefinitionDto { Key = "PercentageComplete", Label = "% concluída", Type = "number" }
                };

                // Vamos montar ao mesmo tempo:
                // - taskDataList: para salvar no banco
                // - nodes: para montar a árvore na resposta
                var taskDataList =
                    new List<(string? Name, string? DurationText, DateTime? Start, DateTime? Finish, double? Percentage)>();
                var nodes = new List<TaskNode>();

                foreach (var task in tasks)
                {
                    if (task == null)
                        continue;

                    var name = task.Name;
                    var duration = task.Duration;
                    var start = task.Start;
                    var finish = task.Finish;
                    double? percentage = task.PercentageComplete;

                    // ignora linhas completamente vazias
                    if (string.IsNullOrWhiteSpace(name) &&
                        start == null &&
                        finish == null &&
                        duration == null)
                    {
                        continue;
                    }

                    var durationText = duration?.ToString();

                    var node = new TaskNode();
                    node.Data["Name"] = name;
                    node.Data["Duration"] = durationText;
                    node.Data["Start"] = NormalizeDate(start);
                    node.Data["Finish"] = NormalizeDate(finish);
                    node.Data["PercentageComplete"] = percentage;

                    var (outline, parent) = GetOutlineInfo(name);
                    node.OutlineKey = outline;
                    node.ParentKey = parent;

                    nodes.Add(node);

                    taskDataList.Add((name, durationText, start, finish, percentage));
                }

                await using var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync(ct);

                // 🔍 Regra: não pode repetir NEM nome, NEM arquivo (.mpp)
                int? existingId = null;
                string? existingName = null;
                string? existingSourceFile = null;

                await using (var checkCmd = new NpgsqlCommand(@"
                    SELECT id, name, source_file_name
                    FROM cronograma_project
                    WHERE name = @name
                       OR source_file_name = @file_name
                    LIMIT 1;", conn))
                {
                    checkCmd.Parameters.AddWithValue("name", finalProjectName);
                    checkCmd.Parameters.AddWithValue("file_name", file.FileName);

                    await using var r = await checkCmd.ExecuteReaderAsync(ct);
                    if (await r.ReadAsync(ct))
                    {
                        existingId = r.GetInt32(0);
                        existingName = r.GetString(1);
                        existingSourceFile = r.GetString(2);
                    }
                }

                if (existingId.HasValue)
                {
                    bool nameConflict = string.Equals(existingName, finalProjectName, StringComparison.Ordinal);
                    bool fileConflict = string.Equals(existingSourceFile, file.FileName, StringComparison.Ordinal);

                    if (nameConflict && fileConflict)
                    {
                        return Conflict(
                            $"Já existe um projeto com o nome '{existingName}' e arquivo '{existingSourceFile}' (id {existingId}).");
                    }
                    else if (nameConflict)
                    {
                        return Conflict(
                            $"Já existe um projeto com o nome '{existingName}' (id {existingId}).");
                    }
                    else if (fileConflict)
                    {
                        return Conflict(
                            $"O arquivo '{existingSourceFile}' já foi utilizado no projeto '{existingName}' (id {existingId}).");
                    }
                }

                // 🔹 Salva no banco (projeto + tarefas) em transação
                int projectId;
                await using (var tx = await conn.BeginTransactionAsync(ct))
                {
                    // Insere o projeto
                    await using (var cmdProject = new NpgsqlCommand(@"
                        INSERT INTO cronograma_project (name, source_file_name)
                        VALUES (@name, @file_name)
                        RETURNING id;", conn, tx))
                    {
                        cmdProject.Parameters.AddWithValue("name", finalProjectName);
                        cmdProject.Parameters.AddWithValue("file_name", file.FileName);

                        var result = await cmdProject.ExecuteScalarAsync(ct);
                        projectId = Convert.ToInt32(result);
                    }

                    // Insere as tarefas (flat no banco)
                    foreach (var t in taskDataList)
                    {
                        await using var cmdTask = new NpgsqlCommand(@"
                            INSERT INTO cronograma_task
                                (project_id, task_name, duration_text, start_timestamp, finish_timestamp, percentage_complete)
                            VALUES
                                (@project_id, @task_name, @duration_text, @start_ts, @finish_ts, @percentage);",
                            conn, tx);

                        cmdTask.Parameters.AddWithValue("project_id", projectId);
                        cmdTask.Parameters.AddWithValue("task_name", (object?)t.Name ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("duration_text", (object?)t.DurationText ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("start_ts", (object?)t.Start ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("finish_ts", (object?)t.Finish ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("percentage", (object?)t.Percentage ?? DBNull.Value);

                        await cmdTask.ExecuteNonQueryAsync(ct);
                    }

                    await tx.CommitAsync(ct);
                }

                // Monta a resposta em árvore
                var rows = BuildHierarchicalRows(nodes);

                var response = new ProjectResponseDto
                {
                    ProjectName = finalProjectName,
                    Columns = columns,
                    Rows = rows
                };

                return Ok(response);
            }
            catch (PostgresException pgEx) when (pgEx.SqlState == "23505")
            {
                return Conflict($"Já existe um projeto com o mesmo nome ou arquivo. Detalhes: {pgEx.MessageText}");
            }
            catch (Exception ex)
            {
                return StatusCode(
                    StatusCodes.Status500InternalServerError,
                    $"Erro ao processar o arquivo .mpp: {ex.Message}"
                );
            }
            finally
            {
                try
                {
                    if (System.IO.File.Exists(tempPath))
                        System.IO.File.Delete(tempPath);
                }
                catch
                {
                    // ignora erro ao deletar temp
                }
            }
        }

        // ============================================================
        // PUT api/cronograma/{projectId}/upload
        // Edita um projeto existente trocando o .mpp (atualiza tarefas)
        // ============================================================
        [HttpPut("{projectId:int}/upload")]
        [Consumes("multipart/form-data")]
        [ProducesResponseType(typeof(ProjectResponseDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<ProjectResponseDto>> UpdateProjectAsync(
            int projectId,
            IFormFile file,
            CancellationToken ct)
        {
            if (file == null || file.Length == 0)
                return BadRequest("Nenhum arquivo enviado.");

            var tempPath = Path.GetTempFileName();

            try
            {
                // Salva o novo .mpp temporariamente
                await using (var fs = System.IO.File.Create(tempPath))
                {
                    await file.CopyToAsync(fs, ct);
                }

                var reader = new UniversalProjectReader();
                var project = reader.Read(tempPath);

                // Vamos manter o MESMO nome do projeto já cadastrado
                string existingName;

                await using var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync(ct);

                // 1) Busca o projeto existente
                await using (var cmdGet = new NpgsqlCommand(@"
                    SELECT name, source_file_name
                    FROM cronograma_project
                    WHERE id = @id;", conn))
                {
                    cmdGet.Parameters.AddWithValue("id", projectId);
                    await using var r = await cmdGet.ExecuteReaderAsync(ct);

                    if (!await r.ReadAsync(ct))
                        return NotFound($"Projeto com id {projectId} não encontrado.");

                    existingName = r.GetString(0);
                    // string oldFileName = r.GetString(1); // se precisar usar depois
                }

                // 2) Regra: não pode usar um arquivo que já está em outro projeto
                int? conflictId = null;
                string? conflictName = null;

                await using (var cmdCheckFile = new NpgsqlCommand(@"
                    SELECT id, name
                    FROM cronograma_project
                    WHERE source_file_name = @file_name
                      AND id <> @id
                    LIMIT 1;", conn))
                {
                    cmdCheckFile.Parameters.AddWithValue("file_name", file.FileName);
                    cmdCheckFile.Parameters.AddWithValue("id", projectId);

                    await using var r = await cmdCheckFile.ExecuteReaderAsync(ct);
                    if (await r.ReadAsync(ct))
                    {
                        conflictId = r.GetInt32(0);
                        conflictName = r.GetString(1);
                    }
                }

                if (conflictId.HasValue)
                {
                    return Conflict(
                        $"O arquivo '{file.FileName}' já está sendo utilizado no projeto '{conflictName}' (id {conflictId}).");
                }

                // 3) Lê as tarefas do NOVO arquivo
                var tasks = project.Tasks.Cast<TaskEntity>().ToList();

                var columns = new List<ColumnDefinitionDto>
                {
                    new ColumnDefinitionDto { Key = "Name",  Label = "Nome da Tarefa",    Type = "string"   },
                    new ColumnDefinitionDto { Key = "Duration", Label = "Duração",       Type = "duration" },
                    new ColumnDefinitionDto { Key = "Start", Label = "Início",            Type = "date"    },
                    new ColumnDefinitionDto { Key = "Finish", Label = "Término",          Type = "date"    },
                    new ColumnDefinitionDto { Key = "PercentageComplete", Label = "% concluída", Type = "number" }
                };

                var taskDataList =
                    new List<(string? Name, string? DurationText, DateTime? Start, DateTime? Finish, double? Percentage)>();
                var nodes = new List<TaskNode>();

                foreach (var task in tasks)
                {
                    if (task == null)
                        continue;

                    var name = task.Name;
                    var duration = task.Duration;
                    var start = task.Start;
                    var finish = task.Finish;
                    double? percentage = task.PercentageComplete;

                    if (string.IsNullOrWhiteSpace(name) &&
                        start == null &&
                        finish == null &&
                        duration == null)
                    {
                        continue;
                    }

                    var durationText = duration?.ToString();

                    var node = new TaskNode();
                    node.Data["Name"] = name;
                    node.Data["Duration"] = durationText;
                    node.Data["Start"] = NormalizeDate(start);
                    node.Data["Finish"] = NormalizeDate(finish);
                    node.Data["PercentageComplete"] = percentage;

                    var (outline, parent) = GetOutlineInfo(name);
                    node.OutlineKey = outline;
                    node.ParentKey = parent;

                    nodes.Add(node);

                    taskDataList.Add((name, durationText, start, finish, percentage));
                }

                // 4) Atualiza o projeto + recria as tarefas em transação
                await using (var tx = await conn.BeginTransactionAsync(ct))
                {
                    // Atualiza o nome do arquivo no projeto (mantém o mesmo name)
                    await using (var cmdUpdateProject = new NpgsqlCommand(@"
                        UPDATE cronograma_project
                        SET source_file_name = @file_name
                        WHERE id = @id;", conn, tx))
                    {
                        cmdUpdateProject.Parameters.AddWithValue("file_name", file.FileName);
                        cmdUpdateProject.Parameters.AddWithValue("id", projectId);
                        await cmdUpdateProject.ExecuteNonQueryAsync(ct);
                    }

                    // Apaga todas as tarefas antigas do projeto
                    await using (var cmdDeleteTasks = new NpgsqlCommand(@"
                        DELETE FROM cronograma_task
                        WHERE project_id = @id;", conn, tx))
                    {
                        cmdDeleteTasks.Parameters.AddWithValue("id", projectId);
                        await cmdDeleteTasks.ExecuteNonQueryAsync(ct);
                    }

                    // Insere as novas tarefas
                    foreach (var t in taskDataList)
                    {
                        await using var cmdTask = new NpgsqlCommand(@"
                            INSERT INTO cronograma_task
                                (project_id, task_name, duration_text, start_timestamp, finish_timestamp, percentage_complete)
                            VALUES
                                (@project_id, @task_name, @duration_text, @start_ts, @finish_ts, @percentage);",
                            conn, tx);

                        cmdTask.Parameters.AddWithValue("project_id", projectId);
                        cmdTask.Parameters.AddWithValue("task_name", (object?)t.Name ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("duration_text", (object?)t.DurationText ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("start_ts", (object?)t.Start ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("finish_ts", (object?)t.Finish ?? DBNull.Value);
                        cmdTask.Parameters.AddWithValue("percentage", (object?)t.Percentage ?? DBNull.Value);

                        await cmdTask.ExecuteNonQueryAsync(ct);
                    }

                    await tx.CommitAsync(ct);
                }

                var rows = BuildHierarchicalRows(nodes);

                var response = new ProjectResponseDto
                {
                    ProjectName = existingName, // mantém o nome original do projeto
                    Columns = columns,
                    Rows = rows
                };

                return Ok(response);
            }
            catch (PostgresException pgEx) when (pgEx.SqlState == "23505")
            {
                return Conflict($"Violação de unicidade ao atualizar o projeto. Detalhes: {pgEx.MessageText}");
            }
            catch (Exception ex)
            {
                return StatusCode(
                    StatusCodes.Status500InternalServerError,
                    $"Erro ao atualizar o projeto: {ex.Message}"
                );
            }
            finally
            {
                try
                {
                    if (System.IO.File.Exists(tempPath))
                        System.IO.File.Delete(tempPath);
                }
                catch
                {
                    // ignora erro ao deletar temp
                }
            }
        }

        // ============================================================
        // GET api/cronograma/{projectId}
        // ============================================================
        [HttpGet("{projectId:int}")]
        [ProducesResponseType(typeof(ProjectResponseDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<ProjectResponseDto>> GetProjectAsync(
            int projectId,
            CancellationToken ct)
        {
            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync(ct);

            string? projectName;
            await using (var cmdProject = new NpgsqlCommand(@"
                SELECT name
                FROM cronograma_project
                WHERE id = @id;", conn))
            {
                cmdProject.Parameters.AddWithValue("id", projectId);
                var obj = await cmdProject.ExecuteScalarAsync(ct);

                if (obj == null)
                    return NotFound($"Projeto com id {projectId} não encontrado.");

                projectName = (string)obj;
            }

            var columns = new List<ColumnDefinitionDto>
            {
                new ColumnDefinitionDto { Key = "Name",  Label = "Nome da Tarefa",    Type = "string"   },
                new ColumnDefinitionDto { Key = "Duration", Label = "Duração",       Type = "duration" },
                new ColumnDefinitionDto { Key = "Start", Label = "Início",            Type = "date"    },
                new ColumnDefinitionDto { Key = "Finish", Label = "Término",          Type = "date"    },
                new ColumnDefinitionDto { Key = "PercentageComplete", Label = "% concluída", Type = "number" }
            };

            var nodes = new List<TaskNode>();

            await using (var cmdTasks = new NpgsqlCommand(@"
                SELECT
                    task_name,
                    duration_text,
                    start_timestamp,
                    finish_timestamp,
                    percentage_complete
                FROM cronograma_task
                WHERE project_id = @project_id
                ORDER BY id;", conn))
            {
                cmdTasks.Parameters.AddWithValue("project_id", projectId);

                await using var reader = await cmdTasks.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var name = reader.IsDBNull(0) ? null : reader.GetString(0);
                    var durationText = reader.IsDBNull(1) ? null : reader.GetString(1);
                    DateTime? start = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2);
                    DateTime? finish = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
                    object? percObj = reader.IsDBNull(4) ? null : reader.GetValue(4);

                    var node = new TaskNode();
                    node.Data["Name"] = name;
                    node.Data["Duration"] = durationText;
                    node.Data["Start"] = NormalizeDate(start);
                    node.Data["Finish"] = NormalizeDate(finish);
                    node.Data["PercentageComplete"] = percObj;

                    var (outline, parent) = GetOutlineInfo(name);
                    node.OutlineKey = outline;
                    node.ParentKey = parent;

                    nodes.Add(node);
                }
            }

            var rows = BuildHierarchicalRows(nodes);

            var response = new ProjectResponseDto
            {
                ProjectName = projectName!,
                Columns = columns,
                Rows = rows
            };

            return Ok(response);
        }

        // ============================================================
        // GET api/cronograma
        // ============================================================
        [HttpGet]
        [ProducesResponseType(typeof(List<ProjectSummaryDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<ProjectSummaryDto>>> GetAllProjectsAsync(
            CancellationToken ct)
        {
            var list = new List<ProjectSummaryDto>();

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(@"
                SELECT id, name
                FROM cronograma_project
                ORDER BY created_at DESC, id DESC;", conn);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var id = reader.GetInt32(0);
                var name = reader.GetString(1);

                list.Add(new ProjectSummaryDto
                {
                    Id = id,
                    Name = name
                });
            }

            return Ok(list);
        }

        // ============================================================
        // DELETE api/cronograma/{projectId}
        // Remove o projeto e todas as tarefas associadas
        // ============================================================
        [HttpDelete("{projectId:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> DeleteProjectAsync(
            int projectId,
            CancellationToken ct)
        {
            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync(ct);

            // Verifica se o projeto existe
            object? exists;
            await using (var cmdCheck = new NpgsqlCommand(@"
                SELECT 1
                FROM cronograma_project
                WHERE id = @id;", conn))
            {
                cmdCheck.Parameters.AddWithValue("id", projectId);
                exists = await cmdCheck.ExecuteScalarAsync(ct);
            }

            if (exists == null)
                return NotFound($"Projeto com id {projectId} não encontrado.");

            // Exclui tarefas + projeto em transação
            await using (var tx = await conn.BeginTransactionAsync(ct))
            {
                // Remove tarefas relacionadas
                await using (var cmdDeleteTasks = new NpgsqlCommand(@"
                    DELETE FROM cronograma_task
                    WHERE project_id = @id;", conn, tx))
                {
                    cmdDeleteTasks.Parameters.AddWithValue("id", projectId);
                    await cmdDeleteTasks.ExecuteNonQueryAsync(ct);
                }

                // Remove o projeto
                await using (var cmdDeleteProject = new NpgsqlCommand(@"
                    DELETE FROM cronograma_project
                    WHERE id = @id;", conn, tx))
                {
                    cmdDeleteProject.Parameters.AddWithValue("id", projectId);
                    await cmdDeleteProject.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
            }

            return NoContent();
        }

        // ============================================================
        // Helper: extrai "1", "1.1", "1.1.1" do início do nome da tarefa
        // ============================================================
        private static (string? outlineKey, string? parentKey) GetOutlineInfo(string? taskName)
        {
            if (string.IsNullOrWhiteSpace(taskName))
                return (null, null);

            var trimmed = taskName.TrimStart();

            var spaceIndex = trimmed.IndexOf(' ');
            if (spaceIndex <= 0)
                return (null, null);

            var token = trimmed.Substring(0, spaceIndex);
            token = token.TrimEnd('.', ')');

            if (string.IsNullOrEmpty(token) || !token.All(c => char.IsDigit(c) || c == '.'))
                return (null, null);

            var outline = token;
            string? parent = null;

            var lastDot = outline.LastIndexOf('.');
            if (lastDot > 0)
            {
                parent = outline.Substring(0, lastDot);
            }
            else
            {
                parent = null;
            }

            return (outline, parent);
        }

        // ============================================================
        // Helper: monta a hierarquia (adiciona Children) a partir da lista
        // ============================================================
        private static List<Dictionary<string, object?>> BuildHierarchicalRows(List<TaskNode> nodes)
        {
            if (nodes.Count == 0)
                return new List<Dictionary<string, object?>>();

            // Mapa de outline -> nó
            var nodeByOutline = nodes
                .Where(n => !string.IsNullOrWhiteSpace(n.OutlineKey))
                .GroupBy(n => n.OutlineKey!)
                .ToDictionary(g => g.Key, g => g.First());

            // Consideramos o primeiro nó SEM outline como "cabeçalho" (ex: "Projeto Cargill")
            TaskNode? header = nodes.FirstOrDefault(n => string.IsNullOrWhiteSpace(n.OutlineKey));

            var roots = new List<TaskNode>();
            if (header != null)
                roots.Add(header);

            foreach (var node in nodes)
            {
                if (node == header)
                    continue;

                // Nó com numeração (1, 1.1, 1.1.1, etc)
                if (!string.IsNullOrWhiteSpace(node.OutlineKey))
                {
                    if (!string.IsNullOrWhiteSpace(node.ParentKey)
                        && nodeByOutline.TryGetValue(node.ParentKey!, out var parent))
                    {
                        parent.Children.Add(node);
                    }
                    else if (header != null)
                    {
                        // Sem pai numérico: fica diretamente abaixo do cabeçalho
                        header.Children.Add(node);
                    }
                    else
                    {
                        // Não tem cabeçalho: vira raiz
                        roots.Add(node);
                    }
                }
                else
                {
                    // Nó sem numeração e sem ser o header principal: raiz
                    roots.Add(node);
                }
            }

            Dictionary<string, object?> ToDictionary(TaskNode n)
            {
                var dict = new Dictionary<string, object?>(n.Data);

                if (n.Children.Count > 0)
                {
                    var children = new List<Dictionary<string, object?>>();
                    foreach (var child in n.Children)
                    {
                        children.Add(ToDictionary(child));
                    }

                    dict["Children"] = children;
                }

                return dict;
            }

            var result = new List<Dictionary<string, object?>>();
            foreach (var root in roots)
            {
                result.Add(ToDictionary(root));
            }

            return result;
        }

        // ============================================================
        // Helper para normalizar data em string ISO
        // ============================================================
        private static string? NormalizeDate(DateTime? value)
        {
            if (value.HasValue)
                return value.Value.ToString("yyyy-MM-ddTHH:mm:ss");

            return null;
        }
    }
}
