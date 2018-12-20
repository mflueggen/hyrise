#pragma once

#include <regex>
#include <string>
#include <vector>

#include <variant>

#include "utils/assert.hpp"

namespace opossum {

/**
 * Wraps an SQL LIKE pattern (e.g. "Hello%Wo_ld") which strings can be tested against.
 *
 * Performance optimizations exist for several simple patterns, such as "Hello%" - which is really just a starts_with()
 * check.
 */
class LikeMatcher {
 public:
  /**
   * Turn SQL LIKE-pattern into a C++ regex.
   */
  static std::string sql_like_to_regex(std::string sql_like);

  static size_t get_index_of_next_wildcard(const std::string& pattern, const size_t offset = 0);
  static bool contains_wildcard(const std::string& pattern);

  explicit LikeMatcher(const std::string& pattern);

  enum class Wildcard { SingleChar /* '_' */, AnyChars /* '%' */ };
  using PatternToken = boost::variant<std::string, Wildcard>;  // Keep type order, users rely on which()
  using PatternTokens = std::vector<PatternToken>;

  /**
   * Turn a pattern string, e.g. "H_llo W%ld" into Tokens {"H", PatternWildcard::SingleChar, "llo W",
   * PatternWildcard::AnyChars, "ld"}
   */
  static PatternTokens pattern_string_to_tokens(const std::string& pattern);

  /**
   * To speed up LIKE there are special implementations available for simple, common patterns.
   * Any other pattern will fall back to regex.
   */
  // 'hello%'
  struct StartsWithPattern final {
    std::string string;
  };
  // '%hello'
  struct EndsWithPattern final {
    std::string string;
  };
  // '%hello%'
  struct ContainsPattern final {
    std::string string;
  };
  // '%hello%world%nice%weather%'
  struct MultipleContainsPattern final {
    std::vector<std::string> strings;
  };

  /**
   * Contains one of the specialised patterns from above (StartsWithPattern, ...) or falls back to std::regex for a
   * general pattern.
   */
  using AllPatternVariant =
    std::variant<std::regex, StartsWithPattern, EndsWithPattern, ContainsPattern, MultipleContainsPattern>;

  static AllPatternVariant pattern_string_to_pattern_variant(const std::string& pattern);

  /**
   * The functor will be called with a concrete matcher.
   * Usage example:
   *    LikeMatcher{"%hello%"}.resolve(false, [](const auto& matcher) {
   *        std::cout << matcher("He said hello!") << std::endl;
   *    }
   */
  template <typename Functor>
  void resolve(const bool invert_results, const Functor& functor) const {
    if (std::holds_alternative<StartsWithPattern>(_pattern_variant)) {
      const auto& prefix = std::get<StartsWithPattern>(_pattern_variant).string;
      functor([&](const std::string& string) -> bool {
        if (string.size() < prefix.size()) return invert_results;
        return (string.compare(0, prefix.size(), prefix) == 0) ^ invert_results;
      });

    } else if (std::holds_alternative<EndsWithPattern>(_pattern_variant)) {
      const auto& suffix = std::get<EndsWithPattern>(_pattern_variant).string;
      functor([&](const std::string& string) -> bool {
        if (string.size() < suffix.size()) return invert_results;
        return (string.compare(string.size() - suffix.size(), suffix.size(), suffix) == 0) ^ invert_results;
      });

    } else if (std::holds_alternative<ContainsPattern>(_pattern_variant)) {
      const auto& contains_str = std::get<ContainsPattern>(_pattern_variant).string;
      functor([&](const std::string& string) -> bool {
        return (string.find(contains_str) != std::string::npos) ^ invert_results;
      });

    } else if (std::holds_alternative<MultipleContainsPattern>(_pattern_variant)) {
      const auto& contains_strs = std::get<MultipleContainsPattern>(_pattern_variant).strings;

      functor([&](const std::string& string) -> bool {
        auto current_position = size_t{0};
        for (const auto& contains_str : contains_strs) {
          current_position = string.find(contains_str, current_position);
          if (current_position == std::string::npos) return invert_results;
          current_position += contains_str.size();
        }
        return !invert_results;
      });

    } else if (std::holds_alternative<std::regex>(_pattern_variant)) {
      const auto& regex = std::get<std::regex>(_pattern_variant);

      functor([&](const std::string& string) -> bool { return std::regex_match(string, regex) ^ invert_results; });

    } else {
      Fail("Pattern not implemented. Probably a bug.");
    }
  }

 private:
  AllPatternVariant _pattern_variant;
};

std::ostream& operator<<(std::ostream& stream, const LikeMatcher::Wildcard& wildcard);

}  // namespace opossum
